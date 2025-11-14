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
-module(merge_raft_test_app).
-moduledoc """
""".
-moduledoc #{author => ["Andrew Bennett <potatosaladx@meta.com>"]}.
-moduledoc #{created => "2025-08-26", modified => "2025-08-26"}.
-moduledoc #{copyright => "Meta Platforms, Inc. and affiliates."}.
-compile(warn_missing_spec_all).
-oncall("whatsapp_clr").

-behaviour(application).

%% application callbacks
-export([
    start/2,
    stop/1
]).

%%%=============================================================================
%%% application callbacks
%%%=============================================================================

-spec start(StartType, StartArgs) -> {ok, Pid} | {ok, Pid, State} | {error, Reason} when
    StartType :: application:start_type(),
    StartArgs :: term(),
    Pid :: pid(),
    State :: term(),
    Reason :: term().
start(_StartType, _StartArgs) ->
    {ok, SupPid} = merge_raft_test_sup:start_link(),
    {ok, SupPid}.

-spec stop(State) -> Ignored when
    State :: term(),
    Ignored :: term().
stop(_State) ->
    ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------
