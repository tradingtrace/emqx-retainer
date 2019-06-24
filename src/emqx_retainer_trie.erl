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

-module(emqx_retainer_trie).

-include("emqx_retainer.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie APIs
-export([ insert/3
        , match/2
        , lookup/1
        , delete/1
        ]).

-export([empty/0]).

%% Mnesia tables
-define(RETAINER_TRIE_NODE, emqx_retainer_trie_node).
-define(RETAINER_TRIE_EDGE, emqx_retainer_trie_edge).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate retainer_trie tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true},
                         {write_concurrency, true}]}],

    ok = ekka_mnesia:create_table(?RETAINER_TRIE_NODE, [
                {ram_copies, [node()]},
                {record_name, retainer_trie_node},
                {attributes, record_info(fields, retainer_trie_node)},
                {storage_properties, StoreProps}]),
    %% Trie edge table
    ok = ekka_mnesia:create_table(?RETAINER_TRIE_EDGE, [
        {type, bag},
        {ram_copies, [node()]},
        {record_name, retainer_trie_edge},
        {attributes, record_info(fields, retainer_trie_edge)},
        {storage_properties, StoreProps}]);

mnesia(copy) ->
    %% Copy retainer_trie_node table
    ok = ekka_mnesia:copy_table(?RETAINER_TRIE_NODE),
    %% Copy retainer_trie_edge table
    ok = ekka_mnesia:copy_table(?RETAINER_TRIE_EDGE).

%%------------------------------------------------------------------------------
%% Trie APIs
%%------------------------------------------------------------------------------

%% @doc Insert a topic filter into the retainer_trie.
-spec(insert(emqx_topic:topic(), emqx_types:message(), non_neg_integer()) -> ok).
insert(Topic, Msg, ExpiryTime) when is_binary(Topic) ->
    case mnesia:wread({?RETAINER_TRIE_NODE, Topic}) of
        [TrieNode] ->
            write_retainer_trie_node(TrieNode#retainer_trie_node{msg = Msg, expiry_time = ExpiryTime});
        [] ->
            %% Add retainer_trie path
            ok = lists:foreach(fun add_path/1, emqx_topic:triples(Topic)),
            %% Add last node
            write_retainer_trie_node(#retainer_trie_node{node_id = Topic, msg = Msg, expiry_time = ExpiryTime})
    end.

%% @doc Find retainer_trie nodes that match the topic name.
-spec(match(emqx_topic:topic(), non_neg_integer()) -> list(emqx_types:messages())).
match(Topic, Now) when is_binary(Topic) ->
    match_node(Now, root, emqx_topic:words(Topic)).

%% @doc Lookup a retainer_trie node.
-spec(lookup(NodeId :: binary()) -> [emqx_types:message()]).
lookup(NodeId) ->
    case mnesia:read(?RETAINER_TRIE_NODE, NodeId) of
        [] -> [];
        [#retainer_trie_node{msg = Msg, expiry_time = ExpiryTime}] ->
            case ExpiryTime =/= 0 andalso ExpiryTime < emqx_time:now_ms() of
                true -> [];
                false -> [Msg]
            end
    end.

%% @doc Delete a topic filter from the retainer_trie.
-spec(delete(emqx_topic:topic()) -> ok).
delete(Topic) when is_binary(Topic) ->
    case mnesia:wread({?RETAINER_TRIE_NODE, Topic}) of
        [#retainer_trie_node{edge_count = 0}] ->
            ok = mnesia:delete({?RETAINER_TRIE_NODE, Topic}),
            delete_path(lists:reverse(emqx_topic:triples(Topic)));
        EdgeList when is_list(EdgeList) ->
            write_retainer_trie_node(#retainer_trie_node{node_id = Topic, msg = undefined})
    end.

%% @doc Is the retainer_trie empty?
-spec(empty() -> boolean()).
empty() ->
    ets:info(?RETAINER_TRIE_NODE, size) == 0.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% @private
%% @doc Add a path to the retainer_trie.
add_path({Parent, _Word, Child}) ->
    Edge = #retainer_trie_edge{parent_id = Parent, child_id = Child},
    case mnesia:wread({?RETAINER_TRIE_NODE, Parent}) of
        [TrieNode = #retainer_trie_node{edge_count = C}] ->
            EdgeList = mnesia:wread({?RETAINER_TRIE_EDGE, Parent}),
            case lists:keymember(Child, #retainer_trie_edge.child_id, EdgeList) of
                true -> ok;
                false ->
                    ok = write_retainer_trie_node(TrieNode#retainer_trie_node{edge_count = C + 1}),
                    ok = write_retainer_trie_edge(Edge)
            end;
        [] ->
            ok = write_retainer_trie_node(#retainer_trie_node{node_id = Parent, edge_count = 1}),
            ok = write_retainer_trie_edge(Edge)
    end.

%% @private
%% @doc Match node with word or '+'.
match_node(Now, root, [NodeId = <<$$, _/binary>>|Words]) ->
    match_node(Now, NodeId, Words, []);

match_node(Now, NodeId, Words) ->
    match_node(Now, NodeId, Words, []).

match_node(Now, NodeId, [], ResAcc) ->
    case mnesia:read(?RETAINER_TRIE_NODE, NodeId) of
        [#retainer_trie_node{msg = undefined}] ->
            ResAcc;
        [#retainer_trie_node{msg = Msg, expiry_time = ExpiryTime}] ->
            case ExpiryTime =/= 0 andalso ExpiryTime < Now of
                true -> ResAcc;
                false -> [Msg|ResAcc]
            end;
        [] -> ResAcc
    end;
match_node(Now, NodeId, ['#'], ResAcc) ->
    EdgeList = mnesia:read(?RETAINER_TRIE_EDGE, NodeId),
    NewResAcc = lists:foldl(fun(#retainer_trie_edge{child_id = Child}, Acc) ->
                    match_node(Now, Child, ['#'], Acc)
                end, ResAcc, EdgeList),
    match_node(Now, NodeId, [], NewResAcc);
match_node(Now, NodeId, ['+'|Words], ResAcc) ->
    EdgeList = mnesia:read(?RETAINER_TRIE_EDGE, NodeId),
    lists:foldl(fun(#retainer_trie_edge{child_id = Child}, Acc) ->
                    match_node(Now, Child, Words, Acc)
                end, ResAcc, EdgeList);
match_node(Now, NodeId, [W|Words], ResAcc) ->
    EdgeList = mnesia:read(?RETAINER_TRIE_EDGE, NodeId),
    Child = case NodeId of
                root -> W;
                _ -> emqx_topic:join([NodeId, W])
            end,
    case lists:keymember(Child, #retainer_trie_edge.child_id, EdgeList) of
        true -> match_node(Now, Child, Words, ResAcc);
        false -> ResAcc
    end.

%% @private
%% @doc Delete paths from the retainer_trie.
delete_path([]) ->
    ok;
delete_path([{Parent, _Word, Child} | RestPath]) ->
    ok = mnesia:delete_object({?RETAINER_TRIE_EDGE, #retainer_trie_edge{parent_id = Parent, child_id = Child}}),
    case mnesia:wread({?RETAINER_TRIE_NODE, Parent}) of
        [#retainer_trie_node{edge_count = 1, msg = undefined}] ->
            ok = mnesia:delete({?RETAINER_TRIE_NODE, Parent}),
            delete_path(RestPath);
        [TrieNode = #retainer_trie_node{edge_count = 1, msg = _}] ->
            write_retainer_trie_node(TrieNode#retainer_trie_node{edge_count = 0});
        [TrieNode = #retainer_trie_node{edge_count = C}] ->
            write_retainer_trie_node(TrieNode#retainer_trie_node{edge_count = C - 1});
        [] ->
            mnesia:abort({node_not_found, Parent})
    end.

%% @private
write_retainer_trie_node(TrieNode) ->
    mnesia:write(?RETAINER_TRIE_NODE, TrieNode, write).

%% @private
write_retainer_trie_edge(Edge) ->
    mnesia:write(?RETAINER_TRIE_EDGE, Edge, write).

