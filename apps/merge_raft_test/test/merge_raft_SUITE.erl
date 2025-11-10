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
-module(merge_raft_SUITE).
-moduledoc """

""".
-moduledoc #{copyright => "Meta Platforms, Inc. and affiliates."}.
-compile(warn_missing_spec_all).
-oncall("whatsapp_clr").

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-behaviour(ct_suite).

%% ct_suite callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1
    % init_per_testcase/2,
    % end_per_testcase/2
]).

%% Test Cases
-export([
    kv/1
]).

%% Macros
-define(WAIT_UNTIL(Condition), ?WAIT_UNTIL(Condition, 5000)).
-define(WAIT_UNTIL(Condition, TimeLimitMs),
    (fun() ->
        ___EndTime = erlang:system_time(millisecond) + TimeLimitMs,
        (fun ___RecFn() ->
            try
                Condition
            catch
                ___Type:___Err:___Stack ->
                    case erlang:system_time(millisecond) >= ___EndTime of
                        true ->
                            erlang:raise(___Type, ___Err, ___Stack);
                        _ ->
                            timer:sleep(20),
                            ___RecFn()
                    end
            end
        end)()
    end)()
).

%%%=============================================================================
%%% ct_suite callbacks
%%%=============================================================================

-spec all() -> merge_raft_test:all().
all() ->
    [
        {group, basic}
    ].

-spec groups() -> merge_raft_test:groups().
groups() ->
    [
        {basic, [parallel], [
            kv
        ]}
    ].

-spec init_per_suite(Config :: ct_suite:ct_config()) -> merge_raft_test:init_per_suite().
init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(merge_raft_test),
    Config.

-spec end_per_suite(Config :: ct_suite:ct_config()) -> merge_raft_test:end_per_suite().
end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% TEST CASES

-spec kv(Config :: ct_suite:ct_config()) -> merge_raft_test:testcase().
kv(_Config) ->
    PeerSet = peer_set_open(?FUNCTION_NAME, 5),
    _ = merge_raft_test_peer_set:cover_call(PeerSet, merge_raft_kv, start, [kv], #{ordered => true}),
    {ok, ok} = merge_raft_test_peer_set:call(PeerSet, 1, merge_raft_kv, sync_put, [?FUNCTION_NAME, a, 1]),
    {ok, ok} = merge_raft_test_peer_set:call(PeerSet, 1, merge_raft_kv, sync_put, [?FUNCTION_NAME, b, 2]),
    % Connect nodes in a chain
    true = merge_raft_test_peer_set:connect(PeerSet, 2, 1),
    true = merge_raft_test_peer_set:connect(PeerSet, 3, 2),
    true = merge_raft_test_peer_set:connect(PeerSet, 4, 3),
    true = merge_raft_test_peer_set:connect(PeerSet, 5, 4),
    % Allow time for cluster merge
    ?WAIT_UNTIL(#{5 := #{1 := _, 4 := _}} = merge_raft_test_peer_set:graph_set(PeerSet), 10_000),
    ?WAIT_UNTIL({ok, 1} = merge_raft_test_peer_set:call(PeerSet, 5, merge_raft_kv, async_get, [?FUNCTION_NAME, a])),
    ?WAIT_UNTIL({ok, 2} = merge_raft_test_peer_set:call(PeerSet, 5, merge_raft_kv, async_get, [?FUNCTION_NAME, b])),
    ?WAIT_UNTIL({ok, 1} = merge_raft_test_peer_set:call(PeerSet, 5, merge_raft_kv, sync_get, [?FUNCTION_NAME, a])),
    ?WAIT_UNTIL({ok, 2} = merge_raft_test_peer_set:call(PeerSet, 5, merge_raft_kv, sync_get, [?FUNCTION_NAME, b])),
    ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

-spec peer_set_open(TestCase, Size) -> PeerSet when
    TestCase :: atom(),
    Size :: pos_integer(),
    PeerSet :: pid().
peer_set_open(TestCase, Size) when is_atom(TestCase) andalso (is_integer(Size) andalso Size >= 1) ->
    Setup = fun({_PeerNode, PeerPid}) ->
        {ok, _} = peer:call(PeerPid, application, ensure_all_started, [merge_raft]),
        ignored
    end,
    merge_raft_test_peer_set:open(erlang:atom_to_binary(TestCase), Size, Setup).
