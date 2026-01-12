-module(mr_cb_test).

-moduledoc """
Testing merge_raft with a simple key-value storage,
 with debugging capabilities.
""".

-behaviour(merge_raft).

%% OTP supervision
-export([
    child_spec/0,
    start_link/0,
    start/0
]).

%% API functions
-export([
    put/3,
    get/2,
    leader_get/2,
    delete/2
]).

%% test functions
-export([
    verify/2,
    sync/1,
    init_cluster/0,
    cleanup/1
]).

%% merge_raft callbacks
-export([
    db_init/2,
    apply_custom/3,
    apply_merge/4,
    apply_leave/3,
    apply_replace/2,
    serialize/1,
    reset/2
]).

%% Debugging
-export([trace/1]).

%% -type server_name() :: merge_raft:server_name().
-type error() :: merge_raft:error().
-type peer() :: merge_raft:peer().
-type members() :: merge_raft:members().
-type commit_metadata() :: merge_raft:commit_metadata().

-type key() :: dynamic().
-type value() :: dynamic().
-type custom_db() :: ets:table().
-type custom_result() :: ok | value().
-type custom_log() :: {put, key(), value()} | {get, key()} | {delete, key()}.
-type custom_db_serialized() :: #{key() => value()}.

%==============================================================================
% OTP supervision
%==============================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    merge_raft:child_spec(#{module => ?MODULE}).

-spec start_link() -> gen_server:start_ret().
start_link() ->
    merge_raft:start_link(#{module => ?MODULE}).

-spec start() -> gen_server:start_ret().
start() ->
    merge_raft:start(#{module => ?MODULE}).

%==============================================================================
% API functions
%==============================================================================

-spec put(pid(), key(), value()) -> {ok, ok} | error().
put(Pid, Key, Value) ->
    merge_raft:commit_sync(Pid, {put, Key, Value}).

-spec get(pid(), key()) -> {ok, value()} | error().
get(Pid, Key) ->
    Tab = persistent_term:get({?MODULE, Pid}),
    case ets:lookup(Tab, Key) of
        [{_Key, Value}] ->
            Value;
        [] ->
            {error, not_found}
    end.

-spec leader_get(pid(), key()) -> {ok, value()} | error().
leader_get(Pid, Key) ->
    case merge_raft:commit_sync(Pid, {get, Key}) of
        {ok, Result} ->
            Result;
        Error ->
            Error
    end.

-spec delete(pid(), key()) -> {ok, ok} | error().
delete(Pid, Key) ->
    merge_raft:commit_sync(Pid, {delete, Key}).

%==============================================================================
% merge_raft callbacks
%==============================================================================

-spec db_init(peer(), _) -> {undefined | commit_metadata(), custom_db()}.
db_init({_, Pid}, _Name) ->
    Tab = ets:new(?MODULE, [set]),
    persistent_term:put({?MODULE, Pid}, Tab),
    {undefined, Tab}.

-spec apply_custom(commit_metadata(), custom_log(), custom_db()) -> {custom_result(), custom_db()}.
apply_custom(_CommitMetadata, {put, Key, Value}, Db) ->
    ets:insert(Db, {Key, Value}),
    {ok, Db};
apply_custom(_CommitMetadata, {get, Key}, Db) ->
    case ets:lookup(Db, Key) of
        [] ->
            {{error, not_found}, Db};
        [{Key, Value}] ->
            {{ok, Value}, Db}
    end;
apply_custom(_CommitMetadata, {delete, Key}, Db) ->
    ets:delete(Db, Key),
    {ok, Db}.

-spec apply_merge(commit_metadata(), members(), custom_db_serialized(), custom_db()) -> custom_db().
apply_merge(_CommitMetadata, _Members, Data, Db) ->
    ets:insert(Db, maps:to_list(Data)),
    Db.

-spec apply_leave(commit_metadata(), peer(), custom_db()) -> custom_db().
apply_leave(_CommitMetadata, _Peer, Db) ->
    Db.

-spec apply_replace(custom_db_serialized(), custom_db()) -> custom_db().
apply_replace(Data, Db) ->
    % can do incremental update to have less churn
    ets:delete_all_objects(Db),
    ets:insert(Db, maps:to_list(Data)),
    Db.

-spec serialize(custom_db()) -> custom_db_serialized().
serialize(Db) ->
    maps:from_list(ets:tab2list(Db)).

-spec reset(peer(), custom_db()) -> custom_db().
reset(_Me, Db) ->
    ets:delete_all_objects(Db),
    Db.

%==============================================================================
% test functions
%==============================================================================

-spec verify([pid()], [{key(), value()}]) -> [].
verify(Pids, Expected) ->
    [] =
        [
            {Pid, Key, PidValue}
         || {Key, Value} <- Expected,
            Pid <- Pids,
            PidValue <- [get(Pid, Key)],
            PidValue =/= Value
        ].

-spec sync([pid()]) -> {non_neg_integer(), [peer()]}.
sync(Pids) ->
    timer:tc(fun() -> sync(Pids, undefined, 40) end).

-spec sync([pid()], term(), non_neg_integer()) -> [peer()].
sync(Pids, Failure, N) when N > 0 ->
    Failure =/= undefined andalso timer:sleep(200),
    Infos = #{Pid => merge_raft:get_info(Pid) || Pid <- Pids},
    case [Info || _Pid := #{role := leader} = Info <- Infos] of
        [LeaderInfo] ->
            case
                map_get(idx_append, LeaderInfo) =:= map_get(idx_apply, LeaderInfo) andalso
                    [
                        {Pid, Key, PidValue}
                     || Key := Value <- maps:without([role, id], LeaderInfo),
                        Pid <- Pids,
                        PidValue <- [map_get(Key, map_get(Pid, Infos))],
                        PidValue =/= Value
                    ]
            of
                false ->
                    sync(Pids, {pending_commit, merge_raft:get_info(hd(Pids))}, N - 1);
                [] ->
                    map_get(member_peers, LeaderInfo);
                Diff ->
                    sync(Pids, Diff, N - 1)
            end;
        _ ->
            sync(Pids, {no_leader, merge_raft:get_info(hd(Pids))}, N - 1)
    end.

-spec init_cluster() -> {[pid()], [reference()]}.
init_cluster() ->
    Pids = [Pid || _ <- lists:seq(1, 5), {ok, Pid} <- [start()]],
    Mons = [monitor(process, Pid) || Pid <- Pids],
    [Pid1, Pid2, Pid3, Pid4, Pid5] = Pids,
    io:format("Network Pids: ~w~n", [Pids]),
    %% trace(#{ps => [Pid1, Pid2, Pid3], fs => all}),
    %% timer:sleep(200),

    {ok, ok} = put(Pid1, a, 1),
    {ok, ok} = put(Pid2, b, 2),

    merge_raft:connect(Pid2, [Pid1]),
    sync([Pid1, Pid2]),
    merge_raft:connect(Pid4, [Pid3]),
    sync([Pid3, Pid4]),
    merge_raft:connect(Pid3, [Pid2]),
    sync([Pid1, Pid2, Pid3, Pid4]),
    merge_raft:connect(Pid5, [Pid4]),
    sync(Pids),
    {Pids, Mons}.

-spec cleanup({[pid()], [reference()]}) -> ok.
cleanup({Pids, Mons}) ->
    [
        receive
            {'DOWN', Mon, process, Pid, Reason} ->
                error({Pid, Reason})
        after 0 ->
                ok
        end
     || Mon <- Mons
    ],
    [exit(Pid, kill) || Pid <- Pids],
    ok.

%==============================================================================
% tracing
%==============================================================================

trace(Options) ->
    dbg:tracer(process, {fun(Msg, State) -> tracer(Msg, State) end, Options}),
    [dbg:p(Pid, [m, c, monotonic_timestamp]) || Pid <- maps:get(ps, Options)],
    dbg:tp(?MODULE, '_', cx),
    case maps:get(fs, Options, all) of
        all ->
            dbg:tpl(merge_raft, '_', cx);
       [_|_] = Fs ->
            [dbg:tpl(merge_raft, F, cx) || F <- Fs]
    end,
    Disable = ['now_ms', 'peer_pid',
               '-append/2-lc$^0/1-0-',
               '-append/2-lc$^1/1-1-',
               '-discover/2-fun-0-',
               '-handle_common/4-fun-0-',
               '-handle_common/4-lc$^1/1-0-',
               '-handle_leader_tick/1-lc$^0/1-0-',
%%               '-handle_leader_tick/1-lc$^0/1-1-',
               '-handle_leader_tick/1-lc$^1/1-1-',
               '-handle_leader_tick/1-lc$^2/1-2-',
               '-insert/3-fun-0-',
               '-maybe_commit/1-lc$^0/1-0-',
               '-maybe_cleanup/1-lc$^0/1-0-',
               '-maybe_send_append/2-lc$^0/1-0-'
              ],
    [dbg:ctpl(merge_raft, F) || F <- Disable],
    ok.

tracer({trace_ts, _From, 'send', {io_request, _, _,Msg}, _To, Time}, State) ->
    io:format("~s DBG_IO: ~n ~s~n",[format_time(Time),format_msg(Msg, State)]),
    State;
tracer({trace_ts, From, 'send', Msg, To, Time}, State) ->
    io:format("~s ~w >> ~w ~s~n",[format_time(Time),From,To,format_msg(Msg, State)]),
    State;
tracer({trace_ts, _Pid, 'receive', _Msg, _Time}, State) ->
    %% io:format("~w:~w ~w << ~s~n",[S,Ms,Pid,format_msg(Msg, State)]),
    State;
tracer({trace_ts, _Pid, 'call', {_, _, [_,tick,_]}, _ST, _Time}, State) ->
    State;
tracer({trace_ts, Pid, 'call', MFA, ST, Time}, State) ->
    io:format("~s   ~w ~s~n",[format_time(Time),Pid, format_call(MFA, ST, State)]),
    State;
tracer({trace_ts, Pid, 'return_from', MFA, Ret, Time}, State) ->
    io:format("~s   ~w ~s~n",[format_time(Time),Pid, format_return(MFA,Ret,State)]),
    State;

tracer(Msg, State) ->
    io:format("UMsg ~p ~n",[Msg]),
    State.

format_msg({call, {Pid, _}, Msg}, State) ->
    io_lib:bformat("call ~w ~s", [Pid, format(Msg, State)]);
format_msg({'$gen_cast', Msg}, State) ->
    io_lib:bformat("cast ~s", [format(Msg, State)]);
format_msg({put_chars, unicode, io_lib, format, [F,A]}, _State) ->
    io_lib:bformat(F, A);
format_msg(Msg, State) ->
    format(Msg, State).

format_call({merge_raft, F, [_Type, Msg, SData]}, _, State)
  when F == leader; F == candidate; F == follower; F == follower_wait ->
    io_lib:bformat("~w << ~s ~s", [F,format(Msg, State),format(SData, State)]);
format_call({merge_raft, debug_format, [Format, Args]}, _, _State) ->
    io_lib:bformat("DBG_IO~n " ++ Format, Args);
format_call({M,F,As}, ST0, State) ->
    Args = lists:join(",", [format(A, State) || A <- As]),
    ST = case maps:get(st, State, false) of
             true -> io_lib:bformat("~w", ST0);
             false -> <<>>
         end,
    io_lib:bformat("~w:~w(~s) ~s", [M,F,Args,ST]).

format_return({merge_raft, handle_common, _A}, _ReturnValue, _State) ->
    ~"return handle_common";
format_return({merge_raft, SName, _A}, ReturnValue, State)
  when SName == leader; SName == follower;
       SName == candidate; SName == follower_wait ->
    try element(1, ReturnValue) of
        keep_state_and_data ->
            io_lib:bformat("~w ~w", [keep_state_and_data, SName]);
        keep_state ->
            io_lib:bformat("~w ~w ~s", [keep_state, SName, format(element(2,ReturnValue), State)]);
        next_state ->
            io_lib:bformat("~w => ~w ~s", [SName, element(2, ReturnValue), format(element(3,ReturnValue), State)])
    catch _:_ when ReturnValue == keep_state_and_data ->
            io_lib:bformat("~w ~w", [keep_state_and_data, SName])
    end;
format_return({M,F,A}, ReturnValue, State) ->
    io_lib:bformat("~w:~w/~w -> ~s", [M,F,A,format(ReturnValue, State)]).

format(SData, _State) when element(1, SData) == sdata ->
    ~"#sdata{}";
format({Int, Pid}, _State) when is_integer(Int), is_pid(Pid) ->
    io_lib:bformat("PEER~w", [Pid]);
format(Tuple, State) when is_tuple(Tuple) ->
    Es = lists:join(",", [format(E, State) || E <- tuple_to_list(Tuple)]),
    io_lib:bformat("{~s}", [Es]);
format(List, State) when length(List) > 0 ->
    Es = lists:join(",", [format(E, State) || E <- List]),
    io_lib:bformat("[~s]", [Es]);
format(Map, State) when is_map(Map) ->
    Es0 = [io_lib:bformat("~s => ~s",
                          [format(K, State), format(V, State)]) || K := V <- Map],
    Es = lists:join(",", Es0),
    io_lib:bformat("#{~s}", [Es]);
format(Msg, _State) ->
    io_lib:bformat("~0.p", [Msg]).

format_time(MonTimeNano) ->
    SysTime = erlang:time_offset(nanosecond) + MonTimeNano,
    Nano = SysTime rem 1000_000_000,
    Sec = SysTime div 1000_000_000,
    {_Data, {H,M,S}} = calendar:system_time_to_local_time(Sec, second),
    io_lib:bformat("~.2.0w:~.2.0w:~.2.0w.~.9.0w", [H,M,S,Nano]).
