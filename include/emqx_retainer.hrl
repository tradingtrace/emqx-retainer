%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-define(APP, emqx_retainer).
-define(TAB, ?APP).
-record(retained, {topic, msg, expiry_time, words, level}).

-type(retainer_trie_node_id() :: binary() | atom()).
-record(retainer_trie_node, {
          node_id          :: retainer_trie_node_id(),
          edge_count = 0   :: non_neg_integer(),
          msg              :: emqx_types:message(),
          expiry_time = 0  :: non_neg_integer()
        }).

-record(retainer_trie_edge, {
          parent_id   :: retainer_trie_node_id(),
          child_id    :: retainer_trie_node_id()
        }).

