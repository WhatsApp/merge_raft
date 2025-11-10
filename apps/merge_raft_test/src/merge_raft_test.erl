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
-module(merge_raft_test).
-moduledoc """

""".
-moduledoc #{author => ["Andrew Bennett <potatosaladx@meta.com>"]}.
-moduledoc #{created => "2025-11-10", modified => "2025-11-10"}.
-moduledoc #{copyright => "Meta Platforms, Inc. and affiliates."}.
-compile(warn_missing_spec_all).
-oncall("whatsapp_clr").

%% Public API
-export([
    dynamic_cast/1
]).

%% Types
-type all() ::
    [TestDef :: ct_suite:ct_test_def()]
    | {skip, Reason :: term()}.
-type end_per_group() ::
    term()
    | {return_group_result, Status :: ct_suite:ct_status()}.
-type end_per_suite() ::
    term()
    | {save_config, SaveConfig :: ct_suite:ct_config()}.
-type end_per_testcase() ::
    term()
    | {fail, Reason :: term()}
    | {save_config, SaveConfig :: ct_suite:ct_config()}.
-type groups() :: [GroupDef :: ct_suite:ct_group_def()].
-type init_per_group() ::
    NewConfig ::
    ct_suite:ct_config()
    | {skip, Reason :: term()}.
-type init_per_suite() ::
    NewConfig ::
    ct_suite:ct_config()
    | {skip, Reason :: term()}
    | {skip_and_save, Reason :: term(), SaveConfig :: ct_suite:ct_config()}.
-type init_per_testcase() ::
    NewConfig ::
    ct_suite:ct_config()
    | {fail, Reason :: term()}
    | {skip, Reason :: term()}.
-type suite() ::
    [Info :: ct_suite:ct_info()].
-type testcase() ::
    term()
    | {skip, Reason :: term()}
    | {fail, Reason :: term()}
    | {comment, Comment :: string()}
    | {save_config, SaveConfig :: ct_suite:ct_config()}
    | {skip_and_save, Reason :: term(), SaveConfig :: ct_suite:ct_config()}.
-type testcase_info() ::
    [Info :: ct_suite:ct_info()].

-export_type([
    all/0,
    end_per_group/0,
    end_per_suite/0,
    end_per_testcase/0,
    groups/0,
    init_per_group/0,
    init_per_suite/0,
    init_per_testcase/0,
    suite/0,
    testcase/0,
    testcase_info/0
]).

%%%=============================================================================
%%% Public API functions
%%%=============================================================================

-spec dynamic_cast(term()) -> dynamic().
dynamic_cast(X) -> X.