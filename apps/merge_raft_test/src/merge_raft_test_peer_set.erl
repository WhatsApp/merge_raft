%%%-----------------------------------------------------------------------------
%%% %CopyrightBegin%
%%%
%%% SPDX-License-Identifier: Apache-2.0
%%%
%%% Copyright (c) Meta Platforms, Inc. and affiliates.
%%% Copyright (c) WhatsApp LLC
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%%% %CopyrightEnd%
%%%-----------------------------------------------------------------------------
%%% % @format
-module(merge_raft_test_peer_set).
-moduledoc """
""".
-moduledoc #{author => ["Andrew Bennett <potatosaladx@meta.com>"]}.
-moduledoc #{created => "2025-11-10", modified => "2025-11-10"}.
-moduledoc #{copyright => "Meta Platforms, Inc. and affiliates."}.
-compile(warn_missing_spec_all).
-oncall("whatsapp_clr").

-behaviour(gen_statem).

%% Public API
-export([
    call/3,
    call/4,
    call/5,
    call/6,
    close/1,
    connect/3,
    cover_call/2,
    cover_call/3,
    cover_call/4,
    cover_call/5,
    graph/1,
    graph/2,
    graph_set/1,
    graph_set/2,
    multi_call/3,
    multi_call/4,
    multi_call/5,
    multi_call/6,
    open/2,
    open/3,
    open/4,
    peer/2,
    peer_list/1,
    peers/1
]).
%% OTP callbacks
-export([
    child_spec/0,
    start_link/5
]).
%% gen_statem callbacks
-export([
    callback_mode/0,
    init/1,
    terminate/3
]).
%% gen_statem states
-export([
    booting/3,
    connected/3
]).

%% Records
-record(boot_data, {
    owner :: monitor(),
    label :: label(),
    size :: pos_integer(),
    setup :: setup(),
    teardown :: teardown()
}).
-record(data, {
    owner :: monitor(),
    label :: label(),
    size :: pos_integer(),
    setup :: setup(),
    teardown :: teardown(),
    peers :: #{pos_integer() => peer_data()}
}).
-record(init_data, {
    owner_pid :: pid(),
    label :: label(),
    size :: pos_integer(),
    setup :: setup(),
    teardown :: teardown()
}).
-record(monitor, {
    pid :: pid(),
    ref :: reference()
}).
-record(peer_data, {
    monitor :: monitor(),
    peer :: peer(),
    state :: dynamic()
}).

%% Internal Types
-type boot_data() :: #boot_data{}.
-type data() :: #data{}.
-type init_data() :: #init_data{}.
-type monitor() :: #monitor{}.
-type peer_data() :: #peer_data{}.

%% Types
-type call_options() :: #{
    timeout => timeout()
}.
-type call_result(T) :: {ok, T} | caught_call_exception().
-type caught_call_exception() ::
    {throw, Throw :: term()}
    | {exit, {exception, Reason :: term()}}
    | {error, {exception, Reason :: term(), StackTrace :: [stack_item()]}}
    | {exit, {signal, Reason :: term()}}
    | {error, {?MODULE, Reason :: term()}}.
-type event_content() :: dynamic().
-type graph() :: multi_call_result(graph_nodes()).
-type graph_nodes() :: [index()].
-type graph_set() :: #{index() => #{index() := []}}.
-type index() :: pos_integer().
-type label() :: merge_raft_test_peer:name().
-type multi_call_options() :: #{
    ordered => boolean(),
    timeout => timeout()
}.
-type multi_call_result(T) :: [{index(), call_result(T)}].
-type order() :: non_neg_integer().
-type peer() :: {node(), pid()}.
-type peers() :: #{index() => peer()}.
-opaque request_id() :: reference().
-opaque request_id_collection() :: #{request_id() => {order(), index()}}.
-opaque response_collection(T) :: #{order() => {index(), call_result(T)}}.
-type setup() :: fun((peer()) -> State :: dynamic()).
-type stack_item() ::
    {
        Module :: atom(),
        Function :: atom(),
        Arity :: arity() | (Args :: [term()]),
        Location :: [
            {file, Filename :: string()}
            | {line, Line :: pos_integer()}
        ]
    }.
-type stop_reason() :: dynamic().
-type teardown() :: fun((peer(), State :: dynamic()) -> Ignored :: dynamic()).

-export_type([
    call_options/0,
    call_result/1,
    caught_call_exception/0,
    event_content/0,
    graph/0,
    graph_nodes/0,
    graph_set/0,
    index/0,
    label/0,
    multi_call_options/0,
    multi_call_result/1,
    order/0,
    peer/0,
    peers/0,
    request_id/0,
    request_id_collection/0,
    response_collection/1,
    setup/0,
    stack_item/0,
    stop_reason/0,
    teardown/0
]).

%% Macros
-define(SYNC_RPC_TIMEOUT, 5000).
-define(is_arguments(X), (?is_proper_list(X) andalso length(X) =< 255)).
-define(is_non_neg_integer(X), (is_integer(X) andalso (X) >= 0)).
-define(is_pos_integer(X), (is_integer(X) andalso (X) >= 1)).
-define(is_proper_list(X), (is_list(X) andalso length(X) >= 0)).
-define(is_timeout(X), ((X) =:= 'infinity' orelse ?is_non_neg_integer(X))).

%%%=============================================================================
%%% Public API functions
%%%=============================================================================

-spec call(Pid, Index, Func) -> Result when
    Pid :: pid(),
    Index :: index(),
    Func :: fun(() -> Result),
    Result :: dynamic().
call(Pid, Index, Func) when is_pid(Pid) andalso ?is_pos_integer(Index) andalso is_function(Func, 0) ->
    call(Pid, Index, Func, #{}).

-spec call(Pid, Index, Func, Arguments | Options) -> Result when
    Pid :: pid(),
    Index :: index(),
    Func :: fun((...) -> Result),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: call_options(),
    Result :: dynamic().
call(Pid, Index, Func, Arguments) when
    is_pid(Pid) andalso ?is_pos_integer(Index) andalso ?is_arguments(Arguments) andalso
        is_function(Func, length(Arguments))
->
    call(Pid, Index, Func, Arguments, #{});
call(Pid, Index, Func, Options) when
    is_pid(Pid) andalso ?is_pos_integer(Index) andalso is_function(Func, 0) andalso is_map(Options)
->
    call(Pid, Index, Func, [], Options).

-spec call(Pid, Index, Module | Func, Function | Arguments, Arguments | Options) -> Result when
    Pid :: pid(),
    Index :: index(),
    Module :: module(),
    Func :: fun((...) -> Result),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: call_options(),
    Result :: dynamic().
call(Pid, Index, Module, Function, Arguments) when
    is_pid(Pid) andalso ?is_pos_integer(Index) andalso is_atom(Module) andalso is_atom(Function) andalso
        ?is_arguments(Arguments)
->
    call(Pid, Index, Module, Function, Arguments, #{});
call(Pid, Index, Func, Arguments, Options) when
    is_pid(Pid) andalso ?is_pos_integer(Index) andalso ?is_arguments(Arguments) andalso
        is_function(Func, length(Arguments)) andalso is_map(Options)
->
    call(Pid, Index, erlang, apply, [Func, Arguments], Options).

-spec call(Pid, Index, Module, Function, Arguments, Options) -> Result when
    Pid :: pid(),
    Index :: index(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: call_options(),
    Result :: dynamic().
call(Pid, Index, Module, Function, Arguments, Options) when
    is_pid(Pid) andalso ?is_pos_integer(Index) andalso is_atom(Module) andalso is_atom(Function) andalso
        ?is_arguments(Arguments) andalso is_map(Options)
->
    Timeout = maps:get(timeout, Options, ?SYNC_RPC_TIMEOUT),
    {_PeerNode, PeerPid} = peer(Pid, Index),
    peer:call(PeerPid, Module, Function, Arguments, Timeout).

-spec close(Pid) -> ok when Pid :: pid().
close(Pid) when is_pid(Pid) ->
    case erlang:is_process_alive(Pid) of
        false ->
            ok;
        true ->
            gen_statem:call(Pid, close)
    end.

-spec connect(Pid, AIndex, BIndex | BIndexList) -> boolean() | ignored when
    Pid :: pid(), AIndex :: index(), BIndex :: index(), BIndexList :: [BIndex].
connect(Pid, AIndex, BIndex) when is_pid(Pid) andalso ?is_pos_integer(AIndex) andalso ?is_pos_integer(BIndex) ->
    {ANode, APid} = peer(Pid, AIndex),
    {BNode, BPid} = peer(Pid, BIndex),
    maybe
        true ?= peer:call(APid, net_kernel, connect_node, [BNode]),
        true ?= peer:call(BPid, net_kernel, connect_node, [ANode]),
        true
    end;
connect(Pid, AIndex, [BIndex | BIndexList]) when
    is_pid(Pid) andalso ?is_pos_integer(AIndex) andalso ?is_pos_integer(BIndex)
->
    maybe
        true ?= connect(Pid, AIndex, BIndex),
        connect(Pid, AIndex, BIndexList)
    end;
connect(Pid, AIndex, []) when is_pid(Pid) andalso ?is_pos_integer(AIndex) ->
    true.

-spec cover_call(Pid, Func) -> MultiCallResult when
    Pid :: pid(),
    Func :: fun(() -> Result),
    MultiCallResult :: multi_call_result(Result).
cover_call(Pid, Func) when is_pid(Pid) andalso is_function(Func, 0) ->
    multi_call(Pid, peer_list(Pid), Func).

-spec cover_call(Pid, Func, Arguments | Options) -> MultiCallResult when
    Pid :: pid(),
    Func :: fun((...) -> Result),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: multi_call_options(),
    MultiCallResult :: multi_call_result(Result).
cover_call(Pid, Func, Arguments) when
    is_pid(Pid) andalso ?is_arguments(Arguments) andalso is_function(Func, length(Arguments))
->
    multi_call(Pid, peer_list(Pid), Func, Arguments);
cover_call(Pid, Func, Options) when is_pid(Pid) andalso is_function(Func, 0) andalso is_map(Options) ->
    multi_call(Pid, peer_list(Pid), Func, Options).

-spec cover_call(Pid, Module | Func, Function | Arguments, Arguments | Options) -> MultiCallResult when
    Pid :: pid(),
    Module :: module(),
    Func :: fun((...) -> Result),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: multi_call_options(),
    MultiCallResult :: multi_call_result(Result),
    Result :: dynamic().
cover_call(Pid, Module, Function, Arguments) when
    is_pid(Pid) andalso is_atom(Module) andalso is_atom(Function) andalso ?is_arguments(Arguments)
->
    multi_call(Pid, peer_list(Pid), Module, Function, Arguments);
cover_call(Pid, Func, Arguments, Options) when
    is_pid(Pid) andalso ?is_arguments(Arguments) andalso is_function(Func, length(Arguments)) andalso
        is_map(Options)
->
    multi_call(Pid, peer_list(Pid), Func, Arguments, Options).

-spec cover_call(Pid, Module, Function, Arguments, Options) -> MultiCallResult when
    Pid :: pid(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: multi_call_options(),
    MultiCallResult :: multi_call_result(Result),
    Result :: dynamic().
cover_call(Pid, Module, Function, Arguments, Options) when
    is_pid(Pid) andalso is_atom(Module) andalso is_atom(Function) andalso ?is_arguments(Arguments) andalso
        is_map(Options)
->
    multi_call(Pid, peer_list(Pid), Module, Function, Arguments, Options).

-spec graph(Pid) -> Graph when Pid :: pid(), Graph :: graph().
graph(Pid) when is_pid(Pid) ->
    graph(Pid, #{}).

-spec graph(Pid, Options) -> Graph when Pid :: pid(), Options :: multi_call_options(), Graph :: graph().
graph(Pid, Options) when is_pid(Pid) andalso is_map(Options) ->
    cover_call(Pid, fun graph_nodes/0, Options).

-spec graph_set(Pid) -> GraphSet when Pid :: pid(), GraphSet :: graph_set().
graph_set(Pid) when is_pid(Pid) ->
    graph_set(Pid, #{}).

-spec graph_set(Pid, Options) -> GraphSet when Pid :: pid(), Options :: multi_call_options(), GraphSet :: graph_set().
graph_set(Pid, Options) when is_pid(Pid) andalso is_map(Options) ->
    #{Index => #{Other => [] || Other <- Connected} || {Index, {ok, Connected}} <- graph(Pid, Options)}.

-spec multi_call(Pid, IndexList, Func) -> MultiCallResult when
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Func :: fun(() -> Result),
    MultiCallResult :: multi_call_result(Result).
multi_call(Pid, IndexList = [_ | _], Func) when
    is_pid(Pid) andalso ?is_proper_list(IndexList) andalso is_function(Func, 0)
->
    multi_call(Pid, IndexList, Func, #{}).

-spec multi_call(Pid, IndexList, Func, Arguments | Options) -> MultiCallResult when
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Func :: fun((...) -> Result),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: multi_call_options(),
    MultiCallResult :: multi_call_result(Result).
multi_call(Pid, IndexList = [_ | _], Func, Arguments) when
    is_pid(Pid) andalso ?is_proper_list(IndexList) andalso ?is_arguments(Arguments) andalso
        is_function(Func, length(Arguments))
->
    multi_call(Pid, IndexList, Func, Arguments, #{});
multi_call(Pid, IndexList = [_ | _], Func, Options) when
    is_pid(Pid) andalso ?is_proper_list(IndexList) andalso is_function(Func, 0) andalso is_map(Options)
->
    multi_call(Pid, IndexList, Func, [], Options).

-spec multi_call(Pid, IndexList, Module | Func, Function | Arguments, Arguments | Options) -> MultiCallResult when
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Module :: module(),
    Func :: fun((...) -> Result),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: multi_call_options(),
    MultiCallResult :: multi_call_result(Result).
multi_call(Pid, IndexList = [_ | _], Module, Function, Arguments) when
    is_pid(Pid) andalso ?is_proper_list(IndexList) andalso is_atom(Module) andalso is_atom(Function) andalso
        ?is_arguments(Arguments)
->
    multi_call(Pid, IndexList, Module, Function, Arguments, #{});
multi_call(Pid, IndexList = [_ | _], Func, Arguments, Options) when
    is_pid(Pid) andalso ?is_proper_list(IndexList) andalso ?is_arguments(Arguments) andalso
        is_function(Func, length(Arguments)) andalso is_map(Options)
->
    multi_call(Pid, IndexList, erlang, apply, [Func, Arguments], Options).

-spec multi_call(Pid, IndexList, Module, Function, Arguments, Options) -> MultiCallResult when
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    Options :: multi_call_options(),
    MultiCallResult :: multi_call_result(Result),
    Result :: dynamic().
multi_call(Pid, IndexList = [_ | _], Module, Function, Arguments, Options) when
    is_pid(Pid) andalso ?is_proper_list(IndexList) andalso is_atom(Module) andalso is_atom(Function) andalso
        ?is_arguments(Arguments) andalso is_map(Options)
->
    CallOptions = maps:with([timeout], Options),
    Ordered = maps:get(ordered, Options, false),
    Tag = erlang:make_ref(),
    case Ordered of
        false ->
            multi_call_unordered(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions);
        true ->
            multi_call_ordered(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions)
    end.

-spec open(Label, Size) -> Pid when Label :: label(), Size :: pos_integer(), Pid :: pid().
open(Label, Size) when ?is_pos_integer(Size) andalso is_binary(Label) ->
    open(Label, Size, fun noop_setup/1).

-spec open(Label, Size, Setup) -> Pid when Label :: label(), Size :: pos_integer(), Setup :: setup(), Pid :: pid().
open(Label, Size, Setup) when is_binary(Label) andalso ?is_pos_integer(Size) andalso is_function(Setup, 1) ->
    open(Label, Size, Setup, fun noop_teardown/2).

-spec open(Label, Size, Setup, Teardown) -> Pid when
    Label :: label(), Size :: pos_integer(), Setup :: setup(), Teardown :: teardown(), Pid :: pid().
open(Label, Size, Setup, Teardown) when
    is_binary(Label) andalso ?is_pos_integer(Size) andalso is_function(Setup, 1) andalso is_function(Teardown, 2)
->
    OwnerPid = erlang:self(),
    case merge_raft_test_peer_set_sup:start_child(OwnerPid, Label, Size, Setup, Teardown) of
        {ok, Pid} when is_pid(Pid) ->
            Pid
    end.

-spec peer(Pid, Index) -> Peer when Pid :: pid(), Index :: pos_integer(), Peer :: peer().
peer(Pid, Index) when is_pid(Pid) andalso ?is_pos_integer(Index) ->
    case gen_statem:call(Pid, {peer, Index}) of
        {ok, Peer} ->
            Peer;
        error ->
            erlang:error(badarg, [Pid, Index])
    end.

-spec peer_list(Pid) -> IndexList when Pid :: pid(), IndexList :: [Index], Index :: index().
peer_list(Pid) when is_pid(Pid) ->
    [Index || Index := _ <- maps:iterator(peers(Pid), ordered)].

-spec peers(Pid) -> Peers when
    Pid :: pid(),
    Peers :: peers().
peers(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, peers).

%%%=============================================================================
%%% OTP callbacks
%%%=============================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => undefined,
        start => {?MODULE, start_link, []},
        restart => temporary,
        shutdown => brutal_kill,
        type => worker,
        modules => [?MODULE]
    }.

-spec start_link(OwnerPid, Label, Size, Setup, Teardown) -> gen_statem:start_ret() when
    OwnerPid :: pid(),
    Label :: label(),
    Size :: pos_integer(),
    Setup :: setup(),
    Teardown :: teardown().
start_link(OwnerPid, Label, Size, Setup, Teardown) when
    is_pid(OwnerPid) andalso is_binary(Label) andalso ?is_pos_integer(Size) andalso is_function(Setup, 1) andalso
        is_function(Teardown, 2)
->
    InitData = #init_data{
        owner_pid = OwnerPid,
        label = Label,
        size = Size,
        setup = Setup,
        teardown = Teardown
    },
    gen_statem:start_link(?MODULE, InitData, []).

%%%=============================================================================
%%% gen_statem callbacks
%%%=============================================================================

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() ->
    [state_functions].

-spec init(InitData) -> InitResult when
    InitData :: init_data(),
    State :: booting,
    BootData :: boot_data(),
    InitResult :: gen_statem:init_result(State, BootData).
init(#init_data{
    owner_pid = OwnerPid,
    label = Label,
    size = Size,
    setup = Setup,
    teardown = Teardown
}) ->
    OwnerMon = erlang:monitor(process, OwnerPid),
    Owner = #monitor{pid = OwnerPid, ref = OwnerMon},
    BootData = #boot_data{
        owner = Owner,
        label = Label,
        size = Size,
        setup = Setup,
        teardown = Teardown
    },
    Actions = [
        {next_event, internal, boot}
    ],
    {ok, booting, BootData, Actions}.

-spec terminate(Reason, State, BootData | Data) -> Ignored when
    Reason :: stop_reason(), State :: booting | connected, BootData :: boot_data(), Data :: data(), Ignored :: term().
terminate(_Reason, _State = booting, _BootData = #boot_data{}) ->
    ok;
terminate(_Reason, _State = connected, _Data = #data{}) ->
    ok.

%%%=============================================================================
%%% gen_statem states
%%%=============================================================================

-spec booting(EventType, EventContent, BootData) -> HandleEventResult when
    EventType :: gen_statem:event_type(),
    EventContent :: event_content(),
    State :: booting | connected,
    BootData :: boot_data(),
    Data :: data(),
    HandleEventResult :: gen_statem:event_handler_result(State, BootData | Data).
booting(
    internal, boot, BootData = #boot_data{owner = Owner, label = Label, size = Size, setup = Setup, teardown = Teardown}
) ->
    BasePeerInfo = merge_raft_test_peer:new(Label),
    PeerSet = start_peers(BootData, BasePeerInfo, #{}),
    Data = #data{
        owner = Owner,
        label = Label,
        size = Size,
        setup = Setup,
        teardown = Teardown,
        peers = PeerSet
    },
    {next_state, connected, Data};
booting({call, From}, close, _BootData = #boot_data{}) ->
    Replies = [{reply, From, ok}],
    {stop_and_reply, normal, Replies};
booting({call, _From}, {peer, _Index}, _BootData = #boot_data{}) ->
    Actions = [postpone],
    {keep_state_and_data, Actions};
booting({call, _From}, peers, _BootData = #boot_data{}) ->
    Actions = [postpone],
    {keep_state_and_data, Actions}.

-spec connected(EventType, EventContent, Data) -> HandleEventResult when
    EventType :: gen_statem:event_type(),
    EventContent :: event_content(),
    State :: connected | booting,
    Data :: data(),
    HandleEventResult :: gen_statem:event_handler_result(State, Data | BootData),
    BootData :: boot_data().
connected(
    info,
    {'DOWN', OwnerMon, process, OwnerPid, _Reason},
    Data = #data{
        owner = #monitor{pid = OwnerPid, ref = OwnerMon}
    }
) ->
    ok = stop_peers(Data),
    stop;
connected(
    info,
    {'DOWN', _PeerMon, process, _Reason},
    Data = #data{
        owner = Owner,
        label = Label,
        size = Size,
        setup = Setup,
        teardown = Teardown
    }
) ->
    ok = stop_peers(Data),
    BootData = #boot_data{
        owner = Owner,
        label = Label,
        size = Size,
        setup = Setup,
        teardown = Teardown
    },
    Actions = [
        {next_event, internal, boot}
    ],
    {next_state, booting, BootData, Actions};
connected({call, From}, close, Data = #data{}) ->
    ok = stop_peers(Data),
    Replies = [{reply, From, ok}],
    {stop_and_reply, normal, Replies};
connected({call, From}, {peer, Index}, _Data = #data{peers = PeerSet}) ->
    Reply =
        case PeerSet of
            #{Index := #peer_data{peer = Peer}} ->
                {ok, Peer};
            _ ->
                error
        end,
    Actions = [{reply, From, Reply}],
    {keep_state_and_data, Actions};
connected({call, From}, peers, _Data = #data{peers = PeerSet}) ->
    Peers = #{Index => Peer || Index := #peer_data{peer = Peer} <- PeerSet},
    Actions = [{reply, From, Peers}],
    {keep_state_and_data, Actions}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

-spec call_result(ResultType, RequestId, Tag, ResultReason) -> CallResult when
    ResultType :: 'down' | 'spawn_reply',
    RequestId :: request_id(),
    Tag :: reference(),
    ResultReason :: dynamic(),
    CallResult :: call_result(Result),
    Result :: dynamic().
call_result(ResultType, RequestId, Tag, ResultReason) ->
    try
        {ok, result(ResultType, RequestId, Tag, ResultReason)}
    catch
        Class:Reason ->
            {Class, Reason}
    end.

-spec graph_nodes() -> GraphNodes when GraphNodes :: graph_nodes().
graph_nodes() ->
    graph_nodes(erlang:nodes()).

-spec graph_nodes(Nodes) -> GraphNodes when Nodes :: [Node], Node :: node(), GraphNodes :: graph_nodes().
graph_nodes([Node | Nodes]) ->
    try
        #{name := Name} = merge_raft_test_peer:decode(Node),
        Index = erlang:binary_to_integer(lists:last(binary:split(Name, <<"-">>, [global, trim_all]))),
        [Index | graph_nodes(Nodes)]
    catch
        _:_ ->
            graph_nodes(Nodes)
    end;
graph_nodes([]) ->
    [].

-spec multi_call_ordered(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions) -> MultiCallResult when
    Tag :: reference(),
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    CallOptions :: call_options(),
    MultiCallResult :: multi_call_result(Result),
    Result :: dynamic().
multi_call_ordered(Tag, Pid, [Index | IndexList], Module, Function, Arguments, CallOptions) ->
    RequestId = multi_call_send_request(Tag, Pid, Index, Module, Function, Arguments, CallOptions),
    CallResult =
        receive
            {Tag, RequestId, error, Reason} ->
                call_result(spawn_reply, RequestId, Tag, Reason);
            {Tag, RequestId, process, _Pid, Reason} ->
                call_result(down, RequestId, Tag, Reason)
        end,
    [{Index, CallResult} | multi_call_ordered(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions)];
multi_call_ordered(_Tag, _Pid, [], _Module, _Function, _Arguments, _CallOptions) ->
    [].

-spec multi_call_receive_responses(Tag, RequestIdCollection, ResponseCollection) -> ResponseCollection when
    Tag :: reference(),
    RequestIdCollection :: request_id_collection(),
    ResponseCollection :: response_collection(Result),
    Result :: dynamic().
multi_call_receive_responses(_Tag, RequestIdCollection, ResponseCollection) when map_size(RequestIdCollection) =:= 0 ->
    ResponseCollection;
multi_call_receive_responses(Tag, RequestIdCollection1, ResponseCollection1) ->
    {Key, CallResult} =
        receive
            {Tag, RequestId, error, Reason} when is_map_key(RequestId, RequestIdCollection1) ->
                {RequestId, call_result(spawn_reply, RequestId, Tag, Reason)};
            {Tag, RequestId, process, _Pid, Reason} when is_map_key(RequestId, RequestIdCollection1) ->
                {RequestId, call_result(down, RequestId, Tag, Reason)}
        end,
    case maps:take(Key, RequestIdCollection1) of
        {{Order, Index}, RequestIdCollection2} when ?is_non_neg_integer(Order) andalso ?is_pos_integer(Index) ->
            ResponseCollection2 = ResponseCollection1#{Order => {Index, CallResult}},
            multi_call_receive_responses(Tag, RequestIdCollection2, ResponseCollection2)
    end.

-spec multi_call_send_request(Tag, Pid, Index, Module, Function, Arguments, CallOptions) -> RequestId when
    Tag :: reference(),
    Pid :: pid(),
    Index :: index(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    CallOptions :: call_options(),
    RequestId :: request_id().
multi_call_send_request(Tag, Pid, Index, Module, Function, Arguments, CallOptions) when
    is_reference(Tag) andalso
        is_pid(Pid) andalso
        ?is_pos_integer(Index) andalso
        is_atom(Module) andalso
        is_atom(Function) andalso
        ?is_arguments(Arguments) andalso
        is_map(CallOptions)
->
    erlang:spawn_request(
        erpc, execute_call, [Tag, ?MODULE, call, [Pid, Index, Module, Function, Arguments, CallOptions]], [
            {monitor, [{tag, Tag}]},
            {reply, error_only},
            {reply_tag, Tag}
        ]
    ).

-spec multi_call_send_requests(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions, RequestIdCollection) ->
    RequestIdCollection
when
    Tag :: reference(),
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    CallOptions :: call_options(),
    RequestIdCollection :: request_id_collection().
multi_call_send_requests(Tag, Pid, [Index | IndexList], Module, Function, Arguments, CallOptions, RequestIdCollection1) ->
    Order = maps:size(RequestIdCollection1),
    RequestId = multi_call_send_request(Tag, Pid, Index, Module, Function, Arguments, CallOptions),
    RequestIdCollection2 = RequestIdCollection1#{RequestId => {Order, Index}},
    multi_call_send_requests(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions, RequestIdCollection2);
multi_call_send_requests(_Tag, _Pid, [], _Module, _Function, _Arguments, _CallOptions, RequestIdCollection) ->
    RequestIdCollection.

-spec multi_call_unordered(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions) -> MultiCallResult when
    Tag :: reference(),
    Pid :: pid(),
    IndexList :: [Index],
    Index :: index(),
    Module :: module(),
    Function :: atom(),
    Arguments :: [Argument],
    Argument :: dynamic(),
    CallOptions :: call_options(),
    MultiCallResult :: multi_call_result(Result),
    Result :: dynamic().
multi_call_unordered(Tag, Pid, IndexList = [_ | _], Module, Function, Arguments, CallOptions) ->
    RequestIdCollection = multi_call_send_requests(Tag, Pid, IndexList, Module, Function, Arguments, CallOptions, #{}),
    ResponseCollection = multi_call_receive_responses(Tag, RequestIdCollection, #{}),
    MultiCallResult = [
        {Index, CallResult}
     || _Order := {Index, CallResult} <- maps:iterator(ResponseCollection, ordered)
    ],
    MultiCallResult.

-spec noop_setup(peer()) -> State :: dynamic().
noop_setup(_Peer) ->
    ignored.

-spec noop_teardown(peer(), State :: dynamic()) -> Ignored :: dynamic().
noop_teardown(_Peer, _State) ->
    ignored.

-dialyzer([{nowarn_function, result/4}, no_return]).

-spec result
    ('down', ReqId, Res, Reason) -> dynamic() when
        ReqId :: reference(),
        Res :: reference(),
        Reason :: dynamic();
    ('spawn_reply', ReqId, Res, Reason) -> no_return() when
        ReqId :: reference(),
        Res :: reference(),
        Reason :: dynamic().
result(down, _ReqId, Res, {Res, return, Return}) ->
    Return;
result(down, _ReqId, Res, {Res, throw, Throw}) ->
    throw(Throw);
result(down, _ReqId, Res, {Res, exit, Exit}) ->
    exit({exception, Exit});
result(down, _ReqId, Res, {Res, error, Error, Stack}) ->
    error({exception, Error, Stack});
result(down, _ReqId, Res, {Res, error, {?MODULE, _} = ErpcErr}) ->
    error(ErpcErr);
result(down, _ReqId, _Res, noconnection) ->
    error({?MODULE, noconnection});
result(down, _ReqId, _Res, Reason) ->
    exit({signal, Reason});
result(spawn_reply, _ReqId, _Res, Reason) ->
    error({?MODULE, Reason}).

-spec start_peers(BootData, BasePeerInfo, PeerSet) -> PeerSet when
    BootData :: boot_data(),
    BasePeerInfo :: merge_raft_test_peer:t(),
    PeerSet :: #{pos_integer() => peer_data()}.
start_peers(#boot_data{size = Size}, _BasePeerInfo, PeerSet) when map_size(PeerSet) >= Size ->
    PeerSet;
start_peers(BootData = #boot_data{label = Label, setup = Setup}, BasePeerInfo, PeerSet1) ->
    Index = maps:size(PeerSet1) + 1,
    PeerInfo = BasePeerInfo#{name => <<Label/bytes, "-", (erlang:integer_to_binary(Index))/bytes, "-">>},
    PeerNode = merge_raft_test_peer:encode(PeerInfo),
    case merge_raft_test_peer_sup:start_child(PeerNode) of
        {ok, PeerPid} when is_pid(PeerPid) ->
            Peer = {PeerNode, PeerPid},
            PeerState = Setup(Peer),
            PeerMon = erlang:monitor(process, PeerPid),
            PeerData = #peer_data{
                monitor = #monitor{pid = PeerPid, ref = PeerMon},
                peer = Peer,
                state = PeerState
            },
            PeerSet2 = PeerSet1#{Index => PeerData},
            start_peers(BootData, BasePeerInfo, PeerSet2)
    end.

-spec stop_peer(Data, PeerData) -> ok when Data :: data(), PeerData :: peer_data().
stop_peer(#data{teardown = Teardown}, #peer_data{
    monitor = #monitor{pid = PeerPid, ref = PeerMon}, peer = Peer, state = PeerState
}) ->
    _ = Teardown(Peer, PeerState),
    _ = erlang:demonitor(PeerMon, [flush]),
    ok = peer:stop(PeerPid),
    ok.

-spec stop_peers(Data) -> ok when Data :: data().
stop_peers(Data = #data{peers = PeerSet}) ->
    PeerIterator = maps:iterator(PeerSet),
    stop_peers(Data, PeerIterator).

-spec stop_peers(Data, PeerIterator) -> ok when
    Data :: data(), PeerIterator :: maps:iterator(Index, PeerData), Index :: pos_integer(), PeerData :: peer_data().
stop_peers(Data = #data{}, PeerIterator1) ->
    case maps:next(PeerIterator1) of
        none ->
            ok;
        {_Index, PeerData, PeerIterator2} ->
            ok = stop_peer(Data, PeerData),
            stop_peers(Data, PeerIterator2)
    end.
