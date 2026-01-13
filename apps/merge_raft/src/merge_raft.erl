%%% % @format

-module(merge_raft).
-compile(warn_missing_spec_all).
-author("zeyu@meta.com").
-oncall("whatsapp_clr").
-moduledoc """
merge_raft behaviour
""".

-behaviour(gen_server).

%% OTP supervision
-export([
    child_spec/1,
    start_link/1,
    start/1
]).

%% API functions
-export([
    commit_async/2,
    commit_sync/2,
    commit_sync_catched/3,
    commit_sync_raw/3
]).

%% Debug functions
-export([
    connect/2,
    get_info/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type custom_log() :: dynamic().
-type custom_db() :: dynamic().
-type custom_db_serialized() :: dynamic().
-type custom_result() :: dynamic().

-type server_name() :: atom().
-type server_module() :: module().
-type node_filter_fun() :: fun((node()) -> boolean()) | undefined.
-type server_type() :: {registered, server_name(), node_filter_fun()} | {dynamic, [gen_server:server_ref()]}.
-type options() :: #{
    module := server_module(),
    type => server_type(),
    target_cluster_size => pos_integer(),
    tick_timeout_ms => pos_integer(),
    heartbeat_timeout_ms => pos_integer(),
    election_timeout_ms => pos_integer(),
    election_variance_ms => pos_integer(),
    reset_timeout_ms => pos_integer(),
    merge_timeout_ms => pos_integer(),
    merge_variance_ms => pos_integer(),
    batch_size => pos_integer(),
    log_history_len => non_neg_integer()
}.
-type error() :: {error, dynamic()}.

-type time_ms() :: non_neg_integer().
-type time_ns() :: non_neg_integer().
-type peer() :: {time_ns(), pid()}.
-type members() :: #{peer() => []}.
-type paused() :: members().
% Future: add read_only
-type role() :: leader | follower | candidate.
-type branch() :: peer().
-type tenure_id() :: non_neg_integer().
-type log_id() :: non_neg_integer().
% log_ref() need to be unique
-type log_ref() :: reference() | gen_server:from().
-type log_message() ::
    {custom, custom_log()}
    | {merge, members(), custom_db_serialized()}
    | {leave, peer()}
    | {leader, peer()}
    | {pause, paused()}.
-type log() :: log_message() | {snapshot, members(), paused(), custom_db_serialized()}.
-type log_value() :: {tenure_id(), log_ref(), log()}.
-type log_entry() :: {log_id(), log_value()}.
-type logs() :: #{log_id() => log_value()}.

-type commit_metadata() :: {branch(), log_id(), tenure_id(), log_ref()}.

-define(REDIRECT, 1).
-define(TICK_MESSAGE, tick).
-define(HEARTBEAT_TIMEOUT(State), (now_ms() + map_get(heartbeat_timeout_ms, State#state.options))).
-define(ELECTION_VARIANCE(State), (now_ms() + rand:uniform(map_get(election_variance_ms, State#state.options)))).
-define(ELECTION_TIMEOUT(State), (?ELECTION_VARIANCE(State) + map_get(election_timeout_ms, State#state.options))).
-define(RESET_TIMEOUT(State), (now_ms() + map_get(reset_timeout_ms, State#state.options))).
-define(TICK_TIMEOUT(State), map_get(tick_timeout_ms, State#state.options)).
-define(MERGE_TIMEOUT(State), (now_ms() + map_get(merge_timeout_ms, State#state.options) +
    rand:uniform(map_get(merge_variance_ms, State#state.options)))).
-define(BATCH_SIZE(State), map_get(batch_size, State#state.options)).
-define(LOG_HISTORY_LEN(State), map_get(log_history_len, State#state.options)).

-define(DBG(F, As), debug_format("~w: ~w: " ++ F, [?LINE, self() | As])).

% Send to state.peers
% Vote if sender tenure is higher or equal and receiver is not voted, even if sender is not in state.peers
-record(vote_request, {
    from :: peer(),
    to :: peer(),
    branch :: branch(),
    tenure_id :: tenure_id(),
    last_log_index :: log_id(),
    last_log_tenure :: tenure_id()
}).

% Store if voted in state.peers
% Count if in member list of most recent committed Log
-record(vote_reply, {
    from :: peer(),
    to :: peer(),
    branch :: branch(),
    tenure_id :: tenure_id(),
    succeeded :: boolean()
}).

% Only send to committed peers
% Accept if sender is leader
% Sender becomes leader if tenure is higher or equal, even if sender is not in state.peers
% Not update state.peers until receive membership updates
-record(append_request, {
    from :: peer(),
    to :: peer(),
    branch :: branch(),
    tenure_id :: tenure_id(),
    prev_log_index :: log_id(),
    prev_log_tenure :: tenure_id(),
    entries :: [log_entry()],
    leader_commit_index :: log_id(),
    leader_cleanup_index :: log_id()
}).

% Store if in state.peers
-record(append_reply, {
    from :: peer(),
    to :: peer(),
    branch :: branch(),
    tenure_id :: tenure_id(),
    result :: success | not_leader | need_older,
    append_id :: log_id()
}).

% Send to peer with largest match index (including not committed?)
% Initialize election if think sender is leader
-record(transfer_leader_request, {
    from :: peer(),
    to :: peer(),
    branch :: branch(),
    tenure_id :: tenure_id()
}).

-record(log_request, {
    redirect = ?REDIRECT :: non_neg_integer(),
    log_ref = make_ref() :: log_ref(),
    log :: log_message()
}).

-record(discover, {
    redirect = ?REDIRECT :: non_neg_integer(),
    from :: peer(),
    branch :: branch(),
    members :: members()
}).

-record(peer_state, {
    base_index = 0 :: log_id(),
    match_index = 0 :: log_id(),
    heartbeat_timeout_ms = 0 :: non_neg_integer(),
    voted = false :: boolean(),
    monitor = undefined :: undefined | reference(),
    % not in original RAFT protocol
    commit_index_sent = 0 :: log_id()
}).

-type member_tree() :: gb_trees:tree(log_id(), members()).
-type peers() :: #{peer() => #peer_state{}}.

-record(state, {
    type :: server_type(),
    module :: server_module(),
    options :: options(),
    me :: peer(),
    role :: role(),
    branch :: branch(),
    tenure_id :: tenure_id(),
    leader :: peer() | undefined,
    voted_for :: peer() | undefined,
    wait_snapshot :: boolean(),
    logs :: logs(),
    append_index :: log_id(),
    commit_index :: log_id(),
    apply_index :: log_id(),
    cleanup_index :: log_id(),
    member_tree :: member_tree(),
    paused :: paused(),
    % holds all the known peers in branch tree until the peer is committed to leave
    peers :: peers(),
    peer_monitors :: #{reference() => peer()},
    election_timeout_ms :: time_ms(),
    merge_timeout_ms :: time_ms(),
    reset_timeout_ms :: time_ms(),
    custom_db :: custom_db(),
    replies :: #{gen_server:from() => []}
}).

-export_type([
    % custom data structures
    custom_log/0,
    custom_db/0,
    custom_db_serialized/0,
    custom_result/0,
    % server API types
    server_name/0,
    server_module/0,
    options/0,
    error/0,
    % commit metadata types
    peer/0,
    members/0,
    branch/0,
    log_id/0,
    tenure_id/0,
    log_ref/0,
    commit_metadata/0
]).

% Initialize an empty db or load from backup
%
% If the callback returns a commit_metadata(),
% the server will pause itself until joined the target branch,
% with assuming all data up to index is known
% To be implemented
-callback db_init(peer(), server_name()) -> {undefined | commit_metadata(), custom_db()}.
% custom_db is expected being able to be serialized and applied to another custom_db as a result of merge
% To be implemented in async way so that large amount of data can be transferred and applied
-callback apply_custom(commit_metadata(), custom_log(), custom_db()) -> {custom_result(), custom_db()}.
% In very rare (unlikely ever happen) racing case the same custom_db_serialized can be applied twice to the custom_db
% It is a design choice to avoid aggregating unlimited data in RAFT
% It is up for user to decide how to handle that, e.g.
% 1. Mark owner for every data and cleanup related data if a member leaves
% 2. Store all past merge histories (unlimited data usage but should be relatively small amount of data)
% 3. Simply allow double merge
-callback apply_merge(commit_metadata(), members(), custom_db_serialized(), custom_db()) -> custom_db().
-callback apply_leave(commit_metadata(), peer(), custom_db()) -> custom_db().
-callback apply_replace(custom_db_serialized(), custom_db()) -> custom_db().
-callback serialize(custom_db()) -> custom_db_serialized().
-callback reset(peer(), custom_db()) -> custom_db().

%==============================================================================
% OTP supervision
%==============================================================================

-spec child_spec(options()) -> supervisor:child_spec().
child_spec(Option) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Option]},
        restart => transient,
        shutdown => 1000,
        modules => [?MODULE]
    }.

-spec start_link(options()) -> gen_server:start_ret().
start_link(#{type := {registered, Name, _Fun}} = Options) ->
    gen_server:start_link({local, Name}, ?MODULE, Options, []);
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

-spec start(options()) -> gen_server:start_ret().
start(#{type := {registered, Name, _Fun}} = Options) ->
    gen_server:start({local, Name}, ?MODULE, Options, []);
start(Options) ->
    gen_server:start(?MODULE, Options, []).

%==============================================================================
% API functions
%==============================================================================

% commit_async can fail if Server dies or leader changed
-spec commit_async(gen_server:server_ref(), custom_log()) -> ok.
commit_async(Server, Log) ->
    gen_server:cast(Server, #log_request{log = {custom, Log}}).

% If commit_sync returns an error(), the request may still eventually got committed in certain cases (timeout, reset).
% If commit_sync returns ok, the request is guaranteed to be committed on Server even if it is a follower
-spec commit_sync(gen_server:server_ref(), custom_log()) -> {ok, custom_result()} | error().
commit_sync(Server, Log) ->
    commit_sync_catched(Server, Log, 5000).

-spec commit_sync_catched(gen_server:server_ref(), custom_log(), timeout()) -> {ok, custom_result()} | error().
commit_sync_catched(Server, Log, Timeout) ->
    try
        gen_server:call(Server, #log_request{log = {custom, Log}}, Timeout)
    catch
        exit:{Reason, _MFA} ->
            {error, Reason};
        Error:Reason:Stack ->
            {error, {Error, Reason, Stack}}
    end.

-spec commit_sync_raw(gen_server:server_ref(), custom_log(), timeout()) -> custom_result().
commit_sync_raw(Server, Log, Timeout) ->
    case commit_sync_catched(Server, Log, Timeout) of
        {ok, Result} ->
            Result;
        {error, Error} ->
            error(Error)
    end.

%==============================================================================
% Debug functions
%==============================================================================

-spec connect(gen_server:server_ref(), [gen_server:server_ref()]) -> ok.
connect(Server, Servers) ->
    gen_server:call(Server, {connect, Servers}).

-spec get_info(gen_server:server_ref()) -> map().
get_info(Server) ->
    gen_server:call(Server, get_info).

%==============================================================================
% gen_server callbacks
%==============================================================================

-spec init(options()) -> {ok, #state{}}.
init(#{module := Module} = Options) ->
    process_flag(trap_exit, true),
    Type = maps:get(type, Options, {dynamic, []}),
    TargetClusterSize = maps:get(target_cluster_size, Options, 5),
    DefaultOptions =
        #{
            heartbeat_timeout_ms => 20 * TargetClusterSize,
            tick_timeout_ms => 25 * TargetClusterSize,
            election_timeout_ms => 5000,
            election_variance_ms => 100 * TargetClusterSize,
            reset_timeout_ms => 300_000,
            merge_timeout_ms => 1000,
            merge_variance_ms => 200 * TargetClusterSize,
            batch_size => 100,
            log_history_len => 1000
        },
    Me = {now_ns(), self()},
    {undefined, CustomDb} =
        case Type of
            {registered, Name, _Fun} ->
                net_kernel:monitor_nodes(true),
                process_flag(async_dist, true),
                % Future: implement read from backup
                Module:db_init(Me, Name);
            {dynamic, _Peers} ->
                Module:db_init(Me, undefined)
        end,
    State = #state{
        type = Type,
        module = Module,
        options = maps:merge(DefaultOptions, Options),
        me = Me,
        role = leader,
        branch = Me,
        tenure_id = 1,
        leader = Me,
        voted_for = Me,
        wait_snapshot = false,
        logs = #{1 => {1, make_ref(), {leader, Me}}},
        append_index = 1,
        commit_index = 1,
        apply_index = 1,
        cleanup_index = 1,
        member_tree = gb_trees:from_orddict([{1, #{Me => []}}]),
        paused = #{},
        peers = #{},
        peer_monitors = #{},
        election_timeout_ms = 0,
        merge_timeout_ms = 0,
        reset_timeout_ms = 0,
        custom_db = CustomDb,
        replies = #{}
    },
    send_discover(State),
    erlang:send_after(?TICK_TIMEOUT(State), self(), ?TICK_MESSAGE),
    {ok, State#state{reset_timeout_ms = ?RESET_TIMEOUT(State)}}.

-spec handle_call
    (#log_request{}, gen_server:from(), #state{}) -> {noreply, #state{}};
    ({connect, [gen_server:server_ref()]}, gen_server:from(), #state{}) -> {reply, ok, #state{}};
    (get_info, gen_server:from(), #state{}) -> {reply, map(), #state{}};
    % bad eqwalizer multi-function support, should be dynamic()
    (dynamic, gen_server:from(), #state{}) -> {reply, not_supported, #state{}}.
handle_call(#log_request{} = LogRequest, From, State) ->
    handle_cast(LogRequest#log_request{log_ref = From}, State);
handle_call({connect, Servers}, _From, State) ->
    [send_discover(Server, State) || Server <- Servers],
    {reply, ok, State};
handle_call(get_info, _From, State) ->
    {reply, get_info_impl(State), State};
handle_call(_Request, _From, State) ->
    {reply, not_supported, State}.

-spec handle_cast(Request, #state{}) -> {noreply, #state{}} when
    Request ::
        #vote_request{}
        | #vote_reply{}
        | #append_request{}
        | #append_reply{}
        | #transfer_leader_request{}
        | #log_request{}
        | #discover{}.
handle_cast(
    #vote_request{
        from = From,
        to = To,
        branch = PeerBranch,
        tenure_id = PeerTenureId,
        last_log_index = PeerLastLogIndex,
        last_log_tenure = PeerLastLogTerm
    },
    State
) when To =:= State#state.me ->
    Succeeded =
        if
            PeerBranch < State#state.branch -> true;
            PeerBranch > State#state.branch -> false;
            %% Equal Branch
            PeerTenureId > State#state.tenure_id -> true;
            PeerTenureId < State#state.tenure_id -> false;
            %% And equal TenureId
            State#state.leader =:= From orelse State#state.leader =:= undefined ->
                case State#state.voted_for of
                    From -> true;
                    undefined ->
                        {PeerLastLogTerm, PeerLastLogIndex} >= {last_log_tenure(State), State#state.append_index};
                    _ -> false
                end;
            true -> false
        end,
    State1 =
        case Succeeded of
            true ->
                (to_follower(PeerBranch, PeerTenureId, State))#state{voted_for = From};
            _ ->
                State
        end,
    peer_send(
        From,
        #vote_reply{
            from = To,
            to = From,
            branch = State1#state.branch,
            tenure_id = State1#state.tenure_id,
            succeeded = Succeeded
        }
    ),
    {noreply, State1};
handle_cast(
    #vote_reply{
        from = From,
        to = To,
        branch = PeerBranch,
        tenure_id = PeerTenureId,
        succeeded = Succeeded
    },
    State
) when To =:= State#state.me ->
    State4 =
        if
            PeerBranch =:= State#state.branch,
            PeerTenureId =:= State#state.tenure_id,
            Succeeded,
            State#state.role =:= candidate,
            is_map_key(From, State#state.peers) ->
                Peers1 = (State#state.peers)#{From => (map_get(From, State#state.peers))#peer_state{voted = true}},
                State1 = State#state{peers = Peers1},
                Members = committed_members(State1),
                Quorum = (map_size(Members) div 2) + 1,
                % Can be optimized to cache vote count but overkill right now
                Votes = [1 || Peer := _ <- Members, Peer =/= State1#state.me, (map_get(Peer, Peers1))#peer_state.voted],
                Voted = length(Votes) + 1,
                if
                    Voted >= Quorum ->
                        Monitors =
                            #{
                                Peer => erlang:monitor(process, Pid, [{tag, follower_down}])
                             || {_, Pid} = Peer := _PeerState <- Peers1
                            },
                        State2 = State1#state{
                            role = leader,
                            leader = State1#state.me,
                            peers =
                                #{
                                    Peer => PeerState#peer_state{
                                        base_index = State1#state.append_index,
                                        monitor = map_get(Peer, Monitors)
                                    }
                                 || Peer := PeerState <- Peers1
                                },
                            peer_monitors = #{Ref => Peer || Peer := Ref <- Monitors}
                        },
                        send_discover(State2),
                        State3 = insert(make_ref(), {leader, State#state.me}, State2),
                        lists:foldl(fun send_empty_append/2, State3, maps:keys(Members) -- [State3#state.me]);
                    true ->
                        State1
                end;
            PeerBranch < State#state.branch;
            (PeerBranch =:= State#state.branch andalso PeerTenureId > State#state.tenure_id) ->
                to_follower(PeerBranch, PeerTenureId, State);
            true ->
                State
        end,
    {noreply, State4};
handle_cast(
    #append_request{
        from = {_, Pid} = From,
        to = To,
        branch = PeerBranch,
        tenure_id = PeerTenureId,
        prev_log_index = PrevLogIndex,
        prev_log_tenure = PrevLogTerm,
        entries = Entries,
        leader_commit_index = LeaderCommitIndex,
        leader_cleanup_index = LeaderCleanupIndex
    },
    State
) when To =:= State#state.me ->
    State1 =
        if
            PeerBranch < State#state.branch;
            (PeerBranch =:= State#state.branch andalso PeerTenureId > State#state.tenure_id) ->
                (to_follower(PeerBranch, PeerTenureId, State))#state{
                    leader = From,
                    voted_for = From,
                    peer_monitors = #{erlang:monitor(process, Pid, [{tag, leader_down}]) => From}
                };
            PeerBranch =:= State#state.branch andalso PeerTenureId =:= State#state.tenure_id ->
                % This should never happen
                State#state.leader =/= undefined andalso State#state.leader =/= From andalso error("wrong leader"),
                State#state{
                    leader = From,
                    % not necessary to update voted_for
                    voted_for =
                        case State#state.voted_for of
                            undefined ->
                                From;
                            _ ->
                                State#state.voted_for
                        end,
                    peer_monitors =
                        case State#state.leader of
                            From ->
                                State#state.peer_monitors;
                            _ ->
                                #{erlang:monitor(process, Pid, [{tag, leader_down}]) => From}
                        end,
                    election_timeout_ms = ?ELECTION_TIMEOUT(State),
                    reset_timeout_ms = ?RESET_TIMEOUT(State)
                };
            true ->
                State
        end,
    Result =
        % From the protocol, we expect append request can only be send from one leader per branch and tenure
        if
            State1#state.branch =/= PeerBranch orelse State1#state.tenure_id =/= PeerTenureId ->
                not_leader;
            % Snapshot append
            PrevLogIndex =:= 0 ->
                success;
            State1#state.wait_snapshot ->
                need_older;
            is_map_key(PrevLogIndex, State1#state.logs),
            element(1, map_get(PrevLogIndex, State1#state.logs)) =:= PrevLogTerm ->
                success;
            true ->
                need_older
        end,
    State2 =
        case Result of
            success ->
                cleanup(LeaderCleanupIndex, commit(LeaderCommitIndex, append(Entries, State1)));
            _ ->
                State1
        end,
    peer_send(
        From,
        #append_reply{
            from = To,
            to = From,
            branch = State2#state.branch,
            tenure_id = State2#state.tenure_id,
            result = Result,
            append_id =
                if
                    State2#state.wait_snapshot ->
                        0;
                    Result =:= success ->
                        State2#state.append_index;
                    true ->
                        min(State2#state.append_index, PrevLogIndex)
                end
        }
    ),
    {noreply, State2};
handle_cast(
    #append_reply{
        from = From,
        to = To,
        branch = PeerBranch,
        tenure_id = PeerTenureId,
        result = Result,
        append_id = PeerAppendId
    },
    State
) when To =:= State#state.me ->
    State2 =
        if
            PeerBranch =:= State#state.branch,
            PeerTenureId =:= State#state.tenure_id,
            State#state.role =:= leader,
            is_map_key(From, State#state.peers) ->
                PeerState = map_get(From, State#state.peers),
                State1 = State#state{
                    peers = (State#state.peers)#{
                        From =>
                            case Result of
                                success ->
                                    PeerState#peer_state{
                                        match_index = max(PeerState#peer_state.match_index, PeerAppendId)
                                    };
                                % Change the sending base index if conflict happens or peer needs older data
                                need_older ->
                                    PeerState#peer_state{base_index = PeerAppendId};
                                not_leader ->
                                    PeerState
                            end
                    }
                },
                maybe_send_append(From, maybe_cleanup(maybe_commit(State1)));
            PeerBranch < State#state.branch;
            (PeerBranch =:= State#state.branch andalso PeerTenureId > State#state.tenure_id) ->
                to_follower(PeerBranch, PeerTenureId, State);
            true ->
                State
        end,
    {noreply, State2};
% Speed up re-election on leader leave.
handle_cast(
    #transfer_leader_request{
        from = From,
        to = To,
        branch = PeerBranch,
        tenure_id = PeerTenureId
    },
    State
) when To =:= State#state.me ->
    State1 =
        if
            PeerBranch < State#state.branch;
            (PeerBranch =:= State#state.branch andalso PeerTenureId > State#state.tenure_id) ->
                to_follower(PeerBranch, PeerTenureId, State);
            true ->
                State
        end,
    State2 =
        case
            PeerBranch =:= State1#state.branch andalso
                PeerTenureId =:= State1#state.tenure_id andalso
                State1#state.leader =:= From
        of
            true ->
                initialize_election(State1);
            false ->
                State1
        end,
    {noreply, State2};
handle_cast(#log_request{log_ref = Ref, log = Log} = LogRequest, State) when State#state.role =:= leader ->
    State1 = maybe_single_member_commit(insert(Ref, Log, maybe_prepare_reply(LogRequest, State))),
    % Future: not always do a immediate send, batch a little bit
    State2 = lists:foldl(fun maybe_send_append/2, State1, maps:keys(committed_members(State1)) -- [State1#state.me]),
    {noreply, State2};
handle_cast(#log_request{redirect = Redirect} = LogRequest, State) when
    State#state.leader =/= undefined,
    Redirect > 0
->
    peer_send(State#state.leader, LogRequest#log_request{redirect = Redirect - 1}),
    {noreply, maybe_prepare_reply(LogRequest, State)};
handle_cast(#log_request{log_ref = Ref}, State) when State#state.leader =:= undefined ->
    {noreply, reply(Ref, {error, no_leader}, State)};
handle_cast(#log_request{log_ref = Ref}, State) when
    State#state.role =:= follower,
    map_size(State#state.peer_monitors) =:= 0
->
    {noreply, reply(Ref, {error, leader_down}, State)};
handle_cast(#log_request{log_ref = Ref}, State) ->
    {noreply, reply(Ref, {error, too_many_redirects}, State)};
handle_cast(#discover{from = From, branch = PeerBranch, members = Members}, State) when
    State#state.role =:= leader andalso not is_map_key(From, State#state.peers) andalso
        map_size(State#state.paused) =:= 0 andalso PeerBranch =/= State#state.branch
->
    State2 =
        case PeerBranch < State#state.branch of
            true ->
                % Newer tree will pause itself and join to older tree
                maybe_single_member_commit(insert(make_ref(), {pause, Members}, State));
            _ ->
                send_discover(From, State),
                State
        end,
    {noreply, State2};
handle_cast(#discover{redirect = Redirect} = DiscoverRequest, State) when
    State#state.leader =/= undefined,
    Redirect > 0
->
    peer_send(State#state.leader, DiscoverRequest#discover{redirect = Redirect - 1}),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(Request, #state{}) -> {noreply, #state{}} | {stop, exit, #state{}} when
    Request :: TickMsg | Nodeup | Nodedown | PeerDown | ExitMsg,
    TickMsg :: ?TICK_MESSAGE,
    Nodeup :: {nodeup, Node :: node()},
    Nodedown :: {nodedown, Node :: node()},
    PeerDown :: {follower_down | leader_down, MonitorRef :: reference(), process, Pid :: pid(), Reason :: term()},
    ExitMsg :: {'EXIT', Pid :: pid(), Reason :: term()}.
% leader tick
handle_info(?TICK_MESSAGE, State) when State#state.role =:= leader ->
    erlang:send_after(?TICK_TIMEOUT(State), self(), ?TICK_MESSAGE),
    State1 = lists:foldl(fun maybe_send_append/2, State, maps:keys(committed_members(State)) -- [State#state.me]),
    {noreply, maybe_reset(maybe_send_merge(maybe_kick_reset_peers(State1)))};
% non-leader tick
handle_info(?TICK_MESSAGE, State) ->
    erlang:send_after(?TICK_TIMEOUT(State), self(), ?TICK_MESSAGE),
    State1 =
        case now_ms() > State#state.election_timeout_ms of
            true ->
                initialize_election(State);
            false ->
                State
        end,
    {noreply, maybe_reset(maybe_send_merge(State1))};
handle_info({_NodeUpDown, Node}, State) when Node =:= node() ->
    {noreply, reset(State)};
handle_info({nodeup, Node}, State) when State#state.role =:= leader ->
    send_discover(Node, State),
    {noreply, State};
handle_info({follower_down, Ref, process, _Pid, _Reason}, State) when
    State#state.role =:= leader,
    is_map_key(Ref, State#state.peer_monitors)
->
    {noreply, insert(make_ref(), {leave, map_get(Ref, State#state.peer_monitors)}, State)};
handle_info({leader_down, Ref, process, _Pid, _Reason}, State) when
    State#state.role =:= follower,
    is_map_key(Ref, State#state.peer_monitors)
->
    State1 =
        % Choose the oldest peer as the new leader
        % This only works well when there is no pending commits
        case State#state.me =:= hd(lists:sort(maps:keys(appended_members(State)) -- [State#state.leader])) of
            true ->
                initialize_election(State);
            _ ->
                State#state{election_timeout_ms = ?ELECTION_VARIANCE(State)}
        end,
    {noreply, State1};
handle_info({'EXIT', _, _}, State) ->
    {stop, exit, State};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(Reason, State) -> ok when
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: #state{}.
terminate(_Reason, State) ->
    maybe_leader_handover(State),
    ok.

%==============================================================================
% leader functions
%==============================================================================

% Try to see if there is any other cluster that we can merge
-spec send_discover(#state{}) -> term().
send_discover(#state{type = {registered, _Name, _NodeFilterFun}} = State) ->
    [send_discover(Node, State) || Node <- nodes()];
send_discover(#state{type = {dynamic, Peers}} = State) ->
    [send_discover(Peer, State) || Peer <- Peers].

-spec send_discover(peer() | node() | gen_server:server_ref(), #state{}) -> term().
send_discover(Target, State) ->
    gen_server:cast(
        case {Target, State#state.type} of
            {Node, {registered, Name, undefined}} when is_atom(Node) ->
                {Name, Node};
            {Node, {registered, Name, NodeFilterFun}} when is_atom(Node) ->
                case NodeFilterFun(Node) of
                    true ->
                        {Name, Node};
                    false ->
                        undefined
                end;
            {{_StartNs, Pid}, _} when is_pid(Pid) ->
                Pid;
            {D, _} ->
                D
        end,
        #discover{
            from = State#state.me,
            branch = State#state.branch,
            members = committed_members(State)
        }
    ).

-spec maybe_kick_reset_peers(#state{}) -> #state{}.
maybe_kick_reset_peers(State) ->
    maps:fold(
        fun
            (_Pid, [_Peer], StateAcc) ->
                StateAcc;
            (_Pid, Peers, StateAcc) ->
                lists:foldl(
                    fun(Peer, StateAcc1) ->
                        insert(make_ref(), {leave, Peer}, StateAcc1)
                    end,
                    StateAcc,
                    lists:delete(lists:max(Peers), Peers)
                )
        end,
        State,
        maps:groups_from_list(fun({_, Pid}) -> Pid end, maps:keys(appended_members(State)))
    ).

-spec maybe_send_append(peer(), #state{}) -> #state{}.
maybe_send_append(_Peer, State) when State#state.role =/= leader ->
    % This should never happen
    error("not leader");
maybe_send_append(Peer, State) ->
    NowMs = now_ms(),
    #peer_state{
        base_index = BaseIndex,
        heartbeat_timeout_ms = HeartbeatTimeoutMs,
        commit_index_sent = CommitIndexSent
    } = map_get(Peer, State#state.peers),
    % It is time to do heartbeat, or we have new data to send
    if
        NowMs > HeartbeatTimeoutMs ->
            send_empty_append(Peer, State);
        % We don't have the log peer need, send snapshot
        not is_map_key(BaseIndex, State#state.logs) ->
            send_snapshot(Peer, State);
        % Regular batch send
        BaseIndex < State#state.append_index; CommitIndexSent < State#state.commit_index ->
            EndIndex = min(BaseIndex + ?BATCH_SIZE(State), State#state.append_index),
            Entries =
                [
                    {LogIndex, maps:get(LogIndex, State#state.logs)}
                 || LogIndex <- lists:seq(BaseIndex + 1, EndIndex)
                ],
            send_append(Peer, Entries, BaseIndex, EndIndex, State);
        true ->
            State
    end.

-spec send_empty_append(peer(), #state{}) -> #state{}.
send_empty_append(Peer, State) ->
    BaseIndex = (map_get(Peer, State#state.peers))#peer_state.base_index,
    send_append(Peer, [], BaseIndex, BaseIndex, State).

-spec send_snapshot(peer(), #state{}) -> #state{}.
send_snapshot(Peer, State) ->
    {LogTenureId, LogRef, _Log} = map_get(State#state.commit_index, State#state.logs),
    Log = {
        snapshot,
        committed_members(State),
        State#state.paused,
        (State#state.module):serialize(State#state.custom_db)
    },
    Entries = [{State#state.commit_index, {LogTenureId, LogRef, Log}}],
    send_append(Peer, Entries, 0, State#state.commit_index, State).

-spec send_append(peer(), [log_entry()], log_id(), log_id(), #state{}) -> #state{}.
send_append(Peer, Entries, BaseIndex, EndIndex, State) ->
    {PrevLogIndex, PrevLogTenure} =
        case State#state.logs of
            #{BaseIndex := {BaseTanureId, _BaseLogRef, _BaseLog}} ->
                {BaseIndex, BaseTanureId};
            _ ->
                {0, 0}
        end,
    peer_send(
        Peer,
        #append_request{
            from = State#state.me,
            to = Peer,
            branch = State#state.branch,
            tenure_id = State#state.tenure_id,
            prev_log_index = PrevLogIndex,
            prev_log_tenure = PrevLogTenure,
            entries = Entries,
            leader_commit_index = State#state.commit_index,
            leader_cleanup_index = State#state.cleanup_index
        }
    ),
    #{Peer := PeerState} = Peers = State#state.peers,
    State#state{
        peers = Peers#{
            Peer := PeerState#peer_state{
                base_index = EndIndex,
                heartbeat_timeout_ms = ?HEARTBEAT_TIMEOUT(State),
                commit_index_sent = State#state.commit_index
            }
        }
    }.

% Append an Log to the tree
-spec insert(log_ref(), log_message(), #state{}) -> #state{}.
insert(_LogRef, _Log, State) when State#state.role =/= leader ->
    % This should never happen
    error("not leader");
insert(LogRef, _Log, State) when map_size(State#state.paused) > 0 ->
    reply(LogRef, {error, paused}, State);
% Questionable: Assume no member change is pending commit?
insert(LogRef, {leave, LeavePeer} = Log, State) ->
    % LeavePeer should be a member of current branch
    case is_map_key(LeavePeer, appended_members(State)) of
        true ->
            append({State#state.append_index + 1, {State#state.tenure_id, LogRef, Log}}, State);
        _ ->
            reply(LogRef, {error, not_joined}, State)
    end;
% Questionable: Assume no member change is pending commit?
insert(LogRef, {merge, Members, _CustomDbSerialized} = Log, State) ->
    % Anyone is not supposed to join twice to the cluster
    % If all members merged and leaved and somehow magically a merge message arrived, we are in trouble
    % It is solvable by passing Branch instead of Members, but it needs to permenately store all the past branches
    % Right now we don't handle this
    case lists:all(fun(Peer) -> not is_map_key(Peer, State#state.peers) end, maps:keys(Members)) of
        true ->
            append({State#state.append_index + 1, {State#state.tenure_id, LogRef, Log}}, State);
        _ ->
            reply(LogRef, {error, duplicated}, State)
    end;
insert(LogRef, Log, State) ->
    append({State#state.append_index + 1, {State#state.tenure_id, LogRef, Log}}, State).

-spec maybe_single_member_commit(#state{}) -> #state{}.
maybe_single_member_commit(State) when State#state.role =/= leader ->
    % This should never happen
    error("not leader");
maybe_single_member_commit(State) ->
    case map_size(committed_members(State)) of
        1 ->
            maybe_cleanup(maybe_commit(State));
        _ ->
            State
    end.

-spec maybe_commit(#state{}) -> #state{}.
maybe_commit(State) ->
    State1 = maybe_commit_impl(State),
    case State1#state.commit_index > State#state.commit_index of
        true ->
            lists:foldl(fun send_empty_append/2, State1, maps:keys(committed_members(State1)) -- [State1#state.me]);
        _ ->
            State1
    end.

-spec maybe_commit_impl(#state{}) -> #state{}.
maybe_commit_impl(State) when State#state.role =/= leader ->
    % This should never happen
    error("not leader");
maybe_commit_impl(State) when State#state.append_index =:= State#state.commit_index ->
    State#state{reset_timeout_ms = ?RESET_TIMEOUT(State)};
maybe_commit_impl(State) ->
    NextIndex = State#state.commit_index + 1,
    {IsMerge, Members} =
        case State#state.logs of
            #{NextIndex := {_TenureId, _LogRef, {merge, _Members, _CustomDbSerialized}}} ->
                % Merge request quorum does not count new members
                {true, get_members(State#state.commit_index, State)};
            _ ->
                {false, get_members(NextIndex, State)}
        end,
    % Can improve performance here but probably an over kill
    % Will be a performance issue in large cluster
    MatchList = lists:sort(
        [State#state.append_index] ++
            [
                (map_get(Peer, State#state.peers))#peer_state.match_index
             || Peer := _ <- Members,
                is_map_key(Peer, State#state.peers)
            ]
    ),
    CommitIndex = lists:nth((map_size(Members) + 1) div 2, MatchList),
    ?DBG(
        "commit ~w(~w) => ~w > ~w = ~w~n",
        [MatchList, (map_size(Members) + 1) div 2, CommitIndex,
            State#state.commit_index, CommitIndex > State#state.commit_index]
    ),
    case
        CommitIndex > State#state.commit_index andalso
            element(1, map_get(CommitIndex, State#state.logs)) =:= State#state.tenure_id
    of
        true when IsMerge ->
            maybe_commit_impl(commit(NextIndex, State#state{reset_timeout_ms = ?RESET_TIMEOUT(State)}));
        true ->
            case gb_trees:larger(NextIndex, State#state.member_tree) of
                {MemberChangeIndex, _} when MemberChangeIndex =< CommitIndex ->
                    maybe_commit_impl(
                        commit(
                            MemberChangeIndex - 1,
                            State#state{reset_timeout_ms = ?RESET_TIMEOUT(State)}
                        )
                    );
                _ ->
                    commit(CommitIndex, State#state{reset_timeout_ms = ?RESET_TIMEOUT(State)})
            end;
        _ ->
            State
    end.

-spec maybe_cleanup(#state{}) -> #state{}.
maybe_cleanup(State) when State#state.role =/= leader ->
    % This should never happen
    error("not leader");
maybe_cleanup(State) ->
    % Cleanup if all members that is not committed leaved got the data
    % with a configurable buffer
    % Can improve performance here but probably an over kill
    CleanupIndex =
        lists:min(
            [State#state.commit_index] ++
                [
                    (map_get(Peer, State#state.peers))#peer_state.match_index
                 || Peer := _ <- get_members(State#state.cleanup_index + 1, State),
                    % peers that is committed to leave is not counted
                    is_map_key(Peer, State#state.peers)
                ]
        ),
    case CleanupIndex > State#state.cleanup_index of
        true ->
            case gb_trees:larger(CleanupIndex, State#state.member_tree) of
                {MemberChangeIndex, _} when MemberChangeIndex =< CleanupIndex ->
                    maybe_cleanup(cleanup(MemberChangeIndex - 1, State));
                _ ->
                    cleanup(CleanupIndex, State)
            end;
        false ->
            State
    end.

%==============================================================================
% candidate functions
%==============================================================================

-spec initialize_election(#state{}) -> #state{}.
initialize_election(State) when State#state.wait_snapshot ->
    % was just merged to another branch but not received data yet
    State#state{election_timeout_ms = ?ELECTION_TIMEOUT(State)};
initialize_election(State) ->
    [erlang:demonitor(Ref) || Ref := _ <- State#state.peer_monitors],
    State1 = State#state{
        role = candidate,
        tenure_id = State#state.tenure_id + 1,
        leader = undefined,
        voted_for = State#state.me,
        election_timeout_ms = ?ELECTION_TIMEOUT(State),
        peers = #{Peer => #peer_state{} || Peer := _ <- State#state.peers}
    },
    lists:foldl(fun send_election/2, State1, maps:keys(State1#state.peers)).

-spec send_election(peer(), #state{}) -> #state{}.
send_election(Peer, State) ->
    peer_send(
        Peer,
        #vote_request{
            from = State#state.me,
            to = Peer,
            branch = State#state.branch,
            tenure_id = State#state.tenure_id,
            last_log_index = State#state.append_index,
            last_log_tenure = last_log_tenure(State)
        }
    ),
    State.

%==============================================================================
% follower functions
%==============================================================================

%==============================================================================
% common functions
%==============================================================================

-spec maybe_prepare_reply(#log_request{}, #state{}) -> #state{}.
maybe_prepare_reply(#log_request{redirect = ?REDIRECT, log_ref = From}, State) when not is_reference(From) ->
    State#state{replies = (State#state.replies)#{From => []}};
maybe_prepare_reply(_LogRequest, State) ->
    State.

-spec reply(log_ref(), {ok, custom_result()} | error(), #state{}) -> #state{}.
reply(LogRef, Message, State) when
    not is_reference(LogRef) andalso (element(1, Message) =:= error orelse is_map_key(LogRef, State#state.replies))
->
    gen_server:reply(LogRef, Message),
    State#state{replies = maps:remove(LogRef, State#state.replies)};
reply(_LogRef, _Message, State) ->
    State.

% Switch to follower when peer tenure is higher than us
-spec to_follower(branch(), tenure_id(), #state{}) -> #state{}.
to_follower(PeerBranch, PeerTenureId, State) ->
    [erlang:demonitor(Ref) || Ref := _ <- State#state.peer_monitors],
    State#state{
        role = follower,
        branch = PeerBranch,
        tenure_id = PeerTenureId,
        leader = undefined,
        voted_for = undefined,
        % Wait for snapshot if my branch is being merged
        % Questionable if we should let candidate send to not committed merge peers
        % Leave it here for now
        wait_snapshot = State#state.wait_snapshot orelse (PeerBranch < State#state.branch),
        peer_monitors = #{},
        election_timeout_ms = ?ELECTION_TIMEOUT(State)
    }.

-spec maybe_leader_handover(#state{}) -> term().
maybe_leader_handover(State) when State#state.role =:= leader ->
    {MaxPeer, _MaxTreeInfo} =
        maps:fold(
            fun
                (Peer, #peer_state{match_index = MatchIndex}, {_, Max}) when MatchIndex > Max ->
                    {Peer, MatchIndex};
                (_, _, Acc) ->
                    Acc
            end,
            {undefined, 0},
            State#state.peers
        ),
    MaxPeer =/= undefined andalso
        peer_send(
            MaxPeer,
            #transfer_leader_request{
                from = State#state.me,
                to = MaxPeer,
                branch = State#state.branch,
                tenure_id = State#state.tenure_id
            }
        );
maybe_leader_handover(_State) ->
    ok.

-spec reset(#state{}) -> #state{}.
reset(#state{me = {OldNs, Pid}} = State) ->
    maybe_leader_handover(State),
    NowNs =
        case {now_ns(), OldNs} of
            {Ns, _} when Ns > OldNs ->
                Ns;
            _ ->
                OldNs + 1
        end,
    Me = {NowNs, Pid},
    [erlang:demonitor(Ref) || Ref := _ <- State#state.peer_monitors],
    [gen_server:reply(Ref, {error, reset}) || Ref := _ <- State#state.replies],
    #state{
        type = State#state.type,
        module = State#state.module,
        options = State#state.options,
        me = Me,
        role = leader,
        branch = Me,
        tenure_id = 1,
        leader = Me,
        voted_for = Me,
        wait_snapshot = false,
        logs = #{1 => {1, make_ref(), {leader, Me}}},
        append_index = 1,
        commit_index = 1,
        apply_index = 1,
        cleanup_index = 1,
        member_tree = gb_trees:from_orddict([{1, #{Me => []}}]),
        paused = #{},
        peers = #{},
        peer_monitors = #{},
        election_timeout_ms = 0,
        merge_timeout_ms = 0,
        reset_timeout_ms = ?RESET_TIMEOUT(State),
        custom_db = (State#state.module):reset(Me, State#state.custom_db),
        replies = #{}
    }.

-spec maybe_reset(#state{}) -> #state{}.
maybe_reset(State) ->
    case now_ms() > State#state.reset_timeout_ms of
        true ->
            reset(State);
        _ ->
            State
    end.

-spec maybe_send_merge(#state{}) -> #state{}.
maybe_send_merge(State) when map_size(State#state.paused) =:= 0 ->
    State;
maybe_send_merge(State) ->
    case now_ms() > State#state.merge_timeout_ms of
        % In current logic, if leader died before pause is committed,
        % then the current branch cannot be successfully merged to another branch anymore
        % Only way to recover is wait for reset
        true when not State#state.wait_snapshot, State#state.append_index =:= State#state.apply_index ->
            Destinations = maps:keys(State#state.paused),
            Dest = lists:nth(rand:uniform(length(Destinations)), Destinations),
            MergeLog = {merge, committed_members(State), (State#state.module):serialize(State#state.custom_db)},
            peer_send(Dest, #log_request{log = MergeLog}),
            State#state{merge_timeout_ms = ?MERGE_TIMEOUT(State)};
        _ ->
            State
    end.

-spec append(log_entry() | [log_entry()], #state{}) -> #state{}.
append([], State) ->
    State;
append([Head | Tail], State) ->
    append(Tail, append(Head, State));
append({LogId, {TenureId, LogRef, _Log}} = LogEntry, State) when is_map_key(LogId, State#state.logs) ->
    case State#state.logs of
        #{LogId := {TenureId, LogRef, _}} ->
            State;
        #{LogId := _} ->
            append(LogEntry, delete(LogId, State))
    end;
% LogId should be append_index + 1, or Log is a snapshot
append({LogId, {_TenureId, _LogRef, Log} = LogValue}, State) ->
    State1 =
        case Log of
            {merge, Members, _CustomDbSerialized} ->
                NewMembers = maps:merge(get_members(LogId - 1, State), Members),
                case State#state.role of
                    leader ->
                        Monitors =
                            #{
                                Peer => erlang:monitor(process, Pid, [{tag, follower_down}])
                             || {_, Pid} = Peer := _ <- Members
                            },
                        State#state{
                            member_tree = gb_trees:insert(LogId, NewMembers, State#state.member_tree),
                            peers = maps:merge(
                                State#state.peers,
                                #{
                                    Peer => #peer_state{
                                        base_index = State#state.append_index,
                                        heartbeat_timeout_ms = ?HEARTBEAT_TIMEOUT(State),
                                        monitor = map_get(Peer, Monitors)
                                    }
                                 || Peer := _ <- Members
                                }
                            ),
                            peer_monitors = maps:merge(
                                State#state.peer_monitors,
                                #{Ref => Peer || Peer := Ref <- Monitors}
                            )
                        };
                    _ ->
                        State#state{
                            member_tree = gb_trees:insert(LogId, NewMembers, State#state.member_tree),
                            peers = maps:merge(State#state.peers, #{Peer => #peer_state{} || Peer := _ <- Members})
                        }
                end;
            {leave, Peer} ->
                NewMembers = maps:remove(Peer, get_members(LogId - 1, State)),
                State#state{member_tree = gb_trees:insert(LogId, NewMembers, State#state.member_tree)};
            {pause, Paused} ->
                State#state{paused = Paused};
            {snapshot, Members, Paused, CustomDbSerialized} ->
                State#state.role =:= leader andalso error("leader append snapshot"),
                CustomDb = (State#state.module):apply_replace(CustomDbSerialized, State#state.custom_db),
                State#state{
                    wait_snapshot = false,
                    logs = #{},
                    commit_index = LogId,
                    apply_index = LogId,
                    cleanup_index = LogId,
                    member_tree = gb_trees:from_orddict([{LogId, Members}]),
                    paused = Paused,
                    peers = #{Peer => #peer_state{} || Peer := _ <- Members, Peer =/= State#state.me},
                    custom_db = CustomDb
                };
            _ ->
                State
        end,
    State1#state{append_index = LogId, logs = (State1#state.logs)#{LogId => LogValue}}.

-spec delete(log_id(), #state{}) -> #state{}.
delete(_LogId, State) when State#state.append_index < State#state.commit_index ->
    % This should never happen
    error("commit conflict");
delete(LogId, State) when State#state.append_index < LogId ->
    State;
delete(_LogId, State) ->
    State1 =
        case map_get(State#state.append_index, State#state.logs) of
            {_TenureId, LogRef, {merge, Members, _CustomDbSerialized}} ->
                State#state{peers = maps:without(maps:keys(Members), State#state.peers)};
            {_TenureId, LogRef, {pause, _Paused}} ->
                State#state{paused = #{}};
            {_TenureId, LogRef, {snapshot, _Members, _Paused, _CustomDbSerialized}} ->
                % This should never happen
                error("snapshot conflict");
            {_TenureId, LogRef, _Log} ->
                State
        end,
    State2 = State1#state{
        logs = maps:remove(State1#state.append_index, State1#state.logs),
        append_index = State1#state.append_index - 1,
        member_tree = gb_trees:delete_any(State1#state.append_index, State1#state.member_tree)
    },
    reply(LogRef, {error, failed}, State2).

-spec commit(log_id(), #state{}) -> #state{}.
commit(LogId, State) ->
    % Future: make this async
    CommitIndex = min(max(LogId, State#state.commit_index), State#state.append_index),
    lists:foldl(
        fun apply_one/2,
        State#state{commit_index = CommitIndex},
        lists:seq(State#state.apply_index + 1, CommitIndex)
    ).

-spec apply_one(log_id(), #state{}) -> #state{}.
apply_one(_LogId, State) ->
    case map_get(State#state.apply_index + 1, State#state.logs) of
        {TenureId, LogRef, {custom, CustomLog}} ->
            CommitMetadata = {State#state.branch, State#state.apply_index, TenureId, LogRef},
            {Result, CustomDb} = (State#state.module):apply_custom(
                CommitMetadata,
                CustomLog,
                State#state.custom_db
            ),
            reply(LogRef, {ok, Result}, State#state{apply_index = State#state.apply_index + 1, custom_db = CustomDb});
        {TenureId, LogRef, {merge, Members, CustomDbSerialized}} ->
            CommitMetadata = {State#state.branch, State#state.apply_index, TenureId, LogRef},
            CustomDb = (State#state.module):apply_merge(
                CommitMetadata,
                Members,
                CustomDbSerialized,
                State#state.custom_db
            ),
            State#state{apply_index = State#state.apply_index + 1, custom_db = CustomDb};
        {_TenureId, _LogRef, {leave, Peer}} when Peer =:= State#state.me ->
            reset(State);
        {TenureId, LogRef, {leave, Peer}} ->
            CommitMetadata = {State#state.branch, State#state.apply_index, TenureId, LogRef},
            CustomDb = (State#state.module):apply_leave(CommitMetadata, Peer, State#state.custom_db),
            State#state{
                apply_index = State#state.apply_index + 1,
                custom_db = CustomDb,
                peers = maps:remove(Peer, State#state.peers),
                peer_monitors =
                    case State#state.peers of
                        #{Peer := #peer_state{monitor = Ref}} ->
                            Ref =/= undefined andalso erlang:demonitor(Ref),
                            maps:remove(Ref, State#state.peer_monitors);
                        _ ->
                            State#state.peer_monitors
                    end
            };
        {_TenureId, _LogRef, {leader, _Peer}} ->
            State#state{apply_index = State#state.apply_index + 1};
        {_TenureId, _LogRef, {pause, _Paused}} ->
            maybe_send_merge(State#state{
                apply_index = State#state.apply_index + 1,
                merge_timeout_ms =
                    case State#state.role of
                        leader ->
                            0;
                        _ ->
                            ?MERGE_TIMEOUT(State)
                    end
            });
        {_TenureId, _LogRef, {snapshot, _Members, _Paused, _CustomDbSerialized}} ->
            % This should never happen
            error("wrong apply")
    end.

-spec cleanup(log_id(), #state{}) -> #state{}.
cleanup(LogId, State) ->
    CleanupIndex = min(max(LogId, State#state.cleanup_index), State#state.commit_index),
    lists:foldl(
        fun(_LogId, StateAcc) ->
            CleanupLogId = StateAcc#state.cleanup_index - ?LOG_HISTORY_LEN(StateAcc),
            StateAcc#state{
                logs = maps:remove(CleanupLogId, StateAcc#state.logs),
                member_tree =
                    case gb_trees:is_defined(CleanupLogId + 1, StateAcc#state.member_tree) of
                        true ->
                            gb_trees:delete_any(CleanupLogId, StateAcc#state.member_tree);
                        _ ->
                            case gb_trees:take_any(CleanupLogId, StateAcc#state.member_tree) of
                                {Members, MemberTree} ->
                                    % eqwalizer:ignore gb_trees:take_any is not dynamic()
                                    gb_trees:insert(CleanupLogId + 1, Members, MemberTree);
                                _ ->
                                    StateAcc#state.member_tree
                            end
                    end,
                cleanup_index = StateAcc#state.cleanup_index + 1
            }
        end,
        State,
        lists:seq(State#state.cleanup_index + 1, CleanupIndex)
    ).

%==============================================================================
% util functions
%==============================================================================
-spec now_ms() -> time_ms().
now_ms() ->
    erlang:system_time(millisecond).

-spec now_ns() -> time_ns().
now_ns() ->
    erlang:system_time(nanosecond).

-spec peer_send(peer(), term()) -> term().
peer_send({_StartNs, Pid}, Msg) ->
    % This required dist_auto_connect
    % Future: can be replaced by callback transports
    % true = net_kernel:connect_node(node(Pid)),
    gen_server:cast(Pid, Msg).

-spec last_log_tenure(#state{}) -> tenure_id().
last_log_tenure(State) ->
    {LastLogTenure, _LastLogRef, _LastLog} = map_get(State#state.append_index, State#state.logs),
    LastLogTenure.

-spec committed_members(#state{}) -> members().
committed_members(State) ->
    case gb_trees:smaller(State#state.commit_index + 1, State#state.member_tree) of
        {_, Members} ->
            Members;
        none ->
            error("bad member tree")
    end.

-spec appended_members(#state{}) -> members().
appended_members(State) ->
    {_, Members} = gb_trees:largest(State#state.member_tree),
    Members.

-spec get_members(log_id(), #state{}) -> members().
get_members(LogId, State) ->
    case gb_trees:smaller(LogId + 1, State#state.member_tree) of
        {_, Members} ->
            Members;
        none ->
            error("bad member tree")
    end.

%==============================================================================
% debug functions
%==============================================================================

-spec get_info_impl(#state{}) -> map().
get_info_impl(State) ->
    #{
        id => State#state.me,
        leader => State#state.leader,
        role => State#state.role,
        idx_tenure => State#state.tenure_id,
        idx_append => State#state.append_index,
        idx_commit => State#state.commit_index,
        idx_apply => State#state.apply_index,
        member_peers => lists:sort([State#state.me | maps:keys(State#state.peers)]),
        member_all => appended_members(State)
    }.

-spec debug_format(string(), list()) -> ok.
debug_format(_F, _As) ->
    %% mr_cb_test tracing will print this
    %% io:format(_F, _As),
    ok.
