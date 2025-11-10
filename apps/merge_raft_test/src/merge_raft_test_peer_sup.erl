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
-module(merge_raft_test_peer_sup).
-moduledoc """
""".
-moduledoc #{author => ["Andrew Bennett <potatosaladx@meta.com>"]}.
-moduledoc #{created => "2025-08-26", modified => "2025-08-26"}.
-moduledoc #{copyright => "Meta Platforms, Inc. and affiliates."}.
-compile(warn_missing_spec_all).
-oncall("whatsapp_clr").

-behaviour(supervisor).

%% OTP callbacks
-export([
    child_spec/0,
    start_child/1,
    start_link/0
]).

%% supervisor callbacks
-export([
    init/1
]).

%%%=============================================================================
%%% OTP callbacks
%%%=============================================================================

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [?MODULE]
    }.

-spec start_child(PeerNode) -> StartChildReturn when
    PeerNode :: node(),
    StartChildReturn :: supervisor:startchild_ret().
start_child(PeerNode) ->
    supervisor:start_child(?MODULE, [PeerNode]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%%%=============================================================================
%%% supervisor callbacks
%%%=============================================================================

-spec init({}) -> InitResult when
    InitResult :: {ok, {SupFlags, [ChildSpec]}} | ignore,
    SupFlags :: supervisor:sup_flags(),
    ChildSpec :: supervisor:child_spec().
init({}) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 0,
        period => 1
    },
    ChildSpecs = [
        merge_raft_test_peer:child_spec()
    ],
    {ok, {SupFlags, ChildSpecs}}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------
