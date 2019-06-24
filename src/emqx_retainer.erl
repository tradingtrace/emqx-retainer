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

-module(emqx_retainer).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/1]).

-export([ load/1
        , unload/0
        ]).

-export([ on_session_subscribed/3
        , on_message_publish/2
        ]).

-export([clean/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ random_words/2
        , match_messages_select/1
        , match_messages_trie/1
        , match_messages3/1
        , match_delete_messages2/1
        , prepare_test/1
        , test_match/1
        , test_match/0
        , test_delete/0
        , write_to_retainer/2
        , write_to_trie/2
        ]).

-record(state, {stats_fun, stats_timer, expiry_timer}).

%%------------------------------------------------------------------------------
%% Load/Unload
%%------------------------------------------------------------------------------

load(Env) ->
    emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/3, []),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

on_session_subscribed(#{client_id := _ClientId}, Topic, #{rh := Rh, first := First}) ->
    if
        Rh =:= 0 orelse (Rh =:= 1 andalso First =:= true) ->
            Msgs = case emqx_topic:wildcard(Topic) of
                       false -> read_messages(Topic);
                       true  -> match_messages(Topic)
                   end,
            dispatch_retained(Topic, Msgs);
        true ->
            ok
    end.

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(Msg = #message{flags   = #{retain := true},
                                  topic   = Topic,
                                  payload = <<>>}, _Env) ->
    mnesia:dirty_delete(?TAB, Topic),
    {ok, Msg};

on_message_publish(Msg = #message{flags = #{retain := true}}, Env) ->
    Msg1 = emqx_message:set_header(retained, true, Msg),
    store_retained(Msg1, Env),
    {ok, Msg};
on_message_publish(Msg, _Env) ->
    {ok, Msg}.

sort_retained(Msgs)  ->
    lists:sort(fun(#message{timestamp = Ts1}, #message{timestamp = Ts2}) ->
                   Ts1 =< Ts2
               end, Msgs).

store_retained(Msg = #message{topic = Topic, payload = Payload, timestamp = Ts}, Env) ->
    case {is_table_full(Env), is_too_big(size(Payload), Env)} of
        {false, false} ->
            ok = emqx_metrics:set('messages.retained', retained_count()),
            ExpiryTime = case Msg of
                #message{topic = <<"$SYS/", _/binary>>} -> 0;
                #message{headers = #{'Message-Expiry-Interval' := Interval}, timestamp = Ts} when Interval =/= 0 ->
                    emqx_time:now_ms(Ts) + Interval * 1000;
                #message{timestamp = Ts} ->
                    case proplists:get_value(expiry_interval, Env, 0) of
                        0 -> 0;
                        Interval -> emqx_time:now_ms(Ts) + Interval
                    end
            end,
            mnesia:dirty_write(?TAB, #retained{topic = Topic, msg = Msg, expiry_time = ExpiryTime});
        {true, _} ->
            ?LOG(error, "[Retainer] Cannot retain message(topic=~s) for table is full!", [Topic]);
        {_, true}->
            ?LOG(error, "[Retainer] Cannot retain message(topic=~s, payload_size=~p) "
                              "for payload is too big!", [Topic, iolist_size(Payload)])
    end.

is_table_full(Env) ->
    Limit = proplists:get_value(max_retained_messages, Env, 0),
    Limit > 0 andalso (retained_count() > Limit).

is_too_big(Size, Env) ->
    Limit = proplists:get_value(max_payload_size, Env, 0),
    Limit > 0 andalso (Size > Limit).

unload() ->
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/3).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link(Env :: list()) -> emqx_types:startlink_ret()).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

clean(Topic) when is_binary(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true -> match_delete_messages(Topic);
        false ->
            Fun = fun() ->
                      case mnesia:read({?TAB, Topic}) of
                          [] -> 0;
                          [_M] -> mnesia:delete({?TAB, Topic}), 1
                      end
                  end,
            {atomic, N} = mnesia:transaction(Fun), N
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Env]) ->
    Copies = case proplists:get_value(storage_type, Env, disc) of
                 ram       -> ram_copies;
                 disc      -> disc_copies;
                 disc_only -> disc_only_copies
             end,
    StoreProps = [{ets, [compressed,
                         {read_concurrency, true},
                         {write_concurrency, true}]},
                  {dets, [{auto_save, 1000}]}],
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {Copies, [node()]},
                {record_name, retained},
                {attributes, record_info(fields, retained)},
                {storage_properties, StoreProps}]),
    ok = ekka_mnesia:copy_table(?TAB),
    case mnesia:table_info(?TAB, storage_type) of
        Copies -> ok;
        _Other ->
            {atomic, ok} = mnesia:change_table_copy_type(?TAB, node(), Copies)
    end,
    StatsFun = emqx_stats:statsfun('retained.count', 'retained.max'),
    {ok, StatsTimer} = timer:send_interval(timer:seconds(1), stats),
    State = #state{stats_fun = StatsFun, stats_timer = StatsTimer},
    {ok, start_expire_timer(proplists:get_value(expiry_interval, Env, 0), State)}.

start_expire_timer(0, State) ->
    State;
start_expire_timer(undefined, State) ->
    State;
start_expire_timer(Ms, State) ->
    {ok, Timer} = timer:send_interval(Ms, expire),
    State#state{expiry_timer = Timer}.

handle_call(Req, _From, State) ->
    ?LOG(error, "[Retainer] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[Retainer] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(retained_count()),
    {noreply, State, hibernate};

handle_info(expire, State) ->
    expire_messages(),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?LOG(error, "[Retainer] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{stats_timer = TRef1, expiry_timer = TRef2}) ->
    timer:cancel(TRef1), timer:cancel(TRef2).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

dispatch_retained(_Topic, []) ->
    ok;
dispatch_retained(Topic, Msgs) ->
    self() ! {dispatch, Topic, sort_retained(Msgs)}.

-spec(read_messages(binary()) -> [emqx_types:message()]).
read_messages(Topic) ->
    case mnesia:dirty_read(?TAB, Topic) of
        [#retained{msg = Msg, expiry_time = 0}] ->
            [Msg];
        [#retained{topic = Topic, msg = Msg, expiry_time = ExpiryTime}] ->
            case emqx_time:now_ms() >= ExpiryTime of
                true ->
                    mnesia:transaction(fun() -> mnesia:delete({?TAB, Topic}) end),
                    [];
                false ->
                    [Msg]
            end;
        [] -> []
    end.

-spec(match_messages(binary()) -> [emqx_types:message()]).
match_messages(Filter) ->
    %% TODO: optimize later...
    Fun = fun
            (#retained{topic = Name, msg = Msg, expiry_time = ExpiryTime}, {Unexpired, Expired}) ->
                case emqx_topic:match(Name, Filter) of
                    true ->
                        case ExpiryTime =/= 0 andalso emqx_time:now_ms() >= ExpiryTime of
                            true -> {Unexpired, [Msg | Expired]};
                            false ->
                                {[Msg | Unexpired], Expired}
                        end;
                    false -> {Unexpired, Expired}
                end
            end,
    {Unexpired, Expired} = mnesia:async_dirty(fun mnesia:foldl/3, [Fun, {[], []}, ?TAB]),
%%    ?LOG(error, "Unexpired: ~w, Expired: ~w~n", [length(Unexpired), length(Expired)]),
    %% Assume a topic A, A is expired when doing foldl, but updated to unexpired before transaction and after foldl
    %% It will be deleted in the below transaction.
    %% What about only delete expired messages in the `expire` timer, not here?
%%    mnesia:transaction(
%%        fun() ->
%%            lists:foreach(fun(Msg) -> mnesia:delete({?TAB, Msg#message.topic}) end, Expired)
%%        end),
    Unexpired.

-spec(match_delete_messages(binary()) -> integer()).
match_delete_messages(Filter) ->
    %% TODO: optimize later...
    Fun = fun(#retained{topic = Name}, Topics) ->
              case emqx_topic:match(Name, Filter) of
                true -> mnesia:delete({?TAB, Name}), [Name | Topics];
                false -> Topics
              end
          end,
    Topics = mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], ?TAB]),
    mnesia:transaction(
        fun() ->
            lists:foreach(fun(Topic) -> mnesia:delete({?TAB, Topic}) end, Topics)
        end),
    length(Topics).

-spec(expire_messages() -> any()).
expire_messages() ->
    NowMs = emqx_time:now_ms(),
    mnesia:transaction(
        fun() ->
            Match = ets:fun2ms(
                        fun(#retained{topic = Topic, expiry_time = ExpiryTime})
                            when ExpiryTime =/= 0 andalso NowMs > ExpiryTime -> Topic
                        end),
            Topics = mnesia:select(?TAB, Match, write),
            lists:foreach(fun(Topic) -> mnesia:delete({?TAB, Topic})
                           end, Topics)
        end).

-spec(retained_count() -> non_neg_integer()).
retained_count() -> mnesia:table_info(?TAB, size).

%%------------------------------------------------------------------------------
%% test
%%------------------------------------------------------------------------------
store_retained_trie(Msg = #message{topic = Topic, payload = Payload, timestamp = Ts}, Env) ->
    case {is_table_full(Env), is_too_big(size(Payload), Env)} of
        {false, false} ->
            ok = emqx_metrics:set('messages.retained', retained_count()),
            ExpiryTime = case Msg of
                             #message{topic = <<"$SYS/", _/binary>>} -> 0;
                             #message{headers = #{'Message-Expiry-Interval' := Interval}, timestamp = Ts} when Interval =/= 0 ->
                                 emqx_time:now_ms(Ts) + Interval * 1000;
                             #message{timestamp = Ts} ->
                                 case proplists:get_value(expiry_interval, Env, 0) of
                                     0 -> 0;
                                     Interval -> emqx_time:now_ms(Ts) + Interval
                                 end
                         end,
            emqx_retainer_trie:insert(Topic, Msg, ExpiryTime);
        {true, _} ->
            ?LOG(error, "[Retainer] Cannot retain message(topic=~s) for table is full!", [Topic]);
        {_, true}->
            ?LOG(error, "[Retainer] Cannot retain message(topic=~s, payload_size=~p) "
                        "for payload is too big!", [Topic, iolist_size(Payload)])
    end.

match_messages_trie(Filter) ->
    Now = emqx_time:now_ms(),
    case emqx_topic:wildcard(Filter) of
        true ->
            mnesia:async_dirty(fun() ->
                                    case catch emqx_retainer_trie:match(Filter, Now) of
                                        Msgs when is_list(Msgs) ->
                                            Msgs;
                                        Error ->
                                            ?LOG(error, "trie match error: ~w~n", [Error]),
                                            ?LOG(error, "trie match backtrace: ~w~n", [erlang:get_stacktrace()])
                                    end
                               end);
        false ->
            mnesia:async_dirty(fun() -> emqx_retainer_trie:lookup(Filter) end)
    end.

store_retained2(Msg = #message{topic = Topic, payload = Payload, timestamp = Ts}, Env) ->
    case {is_table_full(Env), is_too_big(size(Payload), Env)} of
        {false, false} ->
            ok = emqx_metrics:set('messages.retained', retained_count()),
            ExpiryTime = case Msg of
                #message{topic = <<"$SYS/", _/binary>>} -> 0;
                #message{headers = #{'Message-Expiry-Interval' := Interval}, timestamp = Ts} when Interval =/= 0 ->
                    emqx_time:now_ms(Ts) + Interval * 1000;
                #message{timestamp = Ts} ->
                    case proplists:get_value(expiry_interval, Env, 0) of
                        0 -> 0;
                        Interval -> emqx_time:now_ms(Ts) + Interval
                    end
            end,
            Words = list_to_tuple(emqx_topic:words(Topic)),
            mnesia:dirty_write(?TAB, #retained{topic = Topic, msg = Msg, expiry_time = ExpiryTime,
                                               words = Words, level = erlang:size(Words)});
        {true, _} ->
            ?LOG(error, "[Retainer] Cannot retain message(topic=~s) for table is full!", [Topic]);
        {_, true}->
            ?LOG(error, "[Retainer] Cannot retain message(topic=~s, payload_size=~p) "
                              "for payload is too big!", [Topic, iolist_size(Payload)])
    end.

-spec(match_messages_select(binary()) -> [emqx_types:message()]).
match_messages_select(Filter) ->
    MatchExpression = match_expression(Filter, message),
    mnesia:async_dirty(fun mnesia:select/2, [?TAB, MatchExpression]).

-spec(match_expression(binary(), message | topic) -> [tuple()]).
match_expression(Filter, Result) ->
    MatchHead = #retained{topic = '$1', msg = '$2', expiry_time = '$3', words = '$4', level = '$5'},
    Words = emqx_topic:words(Filter),
    Level = erlang:length(Words),
    ExpiryConditions = [{'orelse', {'>=', '$3', emqx_time:now_ms()}, {'=:=', '$3', 0}}],
    {WordsConditions, _} = lists:foldl(fun
                                           ('+', {WCs, N}) -> {WCs, N + 1};
                                           ('#', {WCs, N}) -> {WCs, N + 1};
                                           (W, {WCs, N})  ->
                                               {[{'=:=', W, {element, N, '$4'}}|WCs], N+1}
                                       end, {[], 1}, Words),
    SysConditions = [{'=/=', <<"$SYS">>, {element, 1, '$4'}}],
    LevelConditions = case lists:last(Words) of
                          '#' -> [{'>=', '$5', Level - 1}];
                          _ -> [{'=:=', '$5', Level}]
                      end,
    MatchConditions = lists:flatten([LevelConditions, ExpiryConditions, SysConditions, WordsConditions]),
    MatchBody = case Result of
                    message -> ['$2'];
                    _ -> ['$1']
                end,
    [{MatchHead, MatchConditions, MatchBody}].

-spec(match_messages3(binary()) -> [emqx_types:message()]).
match_messages3(Filter) ->
    %% TODO: optimize later...
    Fun = fun
              (#retained{topic = Name, msg = Msg, expiry_time = ExpiryTime}, Unexpired) ->
                  case emqx_topic:match(Name, Filter) of
                      true ->
                          case ExpiryTime =/= 0 andalso emqx_time:now_ms() >= ExpiryTime of
                              true -> Unexpired;
                              false ->
                                  [Msg | Unexpired]
                          end;
                      false -> Unexpired
                  end
          end,
     mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], ?TAB]).

-spec(match_delete_messages2(binary()) -> integer()).
match_delete_messages2(Filter) ->
    MatchExpression = match_expression(Filter, topic),
    mnesia:transaction(
        fun() ->
            Topics = mnesia:select(?TAB, MatchExpression),
            lists:foldl(fun(Topic, C) ->
                            mnesia:delete({?TAB, Topic}),
                            C + 1
                        end, 0, Topics)
        end).

-define(KEEP_TAG, <<"a">>).
random_words(1, Acc) ->
    [<<"channel">>|Acc];
random_words(2, Acc) ->
    I = integer_to_binary(rand:uniform(10)),
    random_words(1, [<<"room_type_", I/binary>>|Acc]);
random_words(3, Acc) ->
    I = integer_to_binary(rand:uniform(10000)),
    random_words(2, [<<"room_id_", I/binary>>|Acc]);
random_words(4, Acc) ->
    I = integer_to_binary(rand:uniform(100)),
    random_words(3, [<<"measure_", I/binary>>|Acc]);
%%random_words(5, Acc) ->
%%    I = integer_to_binary(rand:uniform(20)),
%%    random_words(4, [<<?KEEP_TAG/binary, I/binary>>|Acc]);
%%random_words(6, Acc) ->
%%    I = integer_to_binary(rand:uniform(40)),
%%    random_words(5, [<<?KEEP_TAG/binary, I/binary>>|Acc]);
%%random_words(7, Acc) ->
%%    I = integer_to_binary(rand:uniform(80)),
%%    random_words(6, [<<?KEEP_TAG/binary, I/binary>>|Acc]);
%%random_words(8, Acc) ->
%%    I = integer_to_binary(rand:uniform(160)),
%%    random_words(7, [<<?KEEP_TAG/binary, I/binary>>|Acc]);
%%random_words(9, Acc) ->
%%    I = integer_to_binary(rand:uniform(100000)),
%%    random_words(8, [<<?KEEP_TAG/binary, I/binary>>|Acc]);
random_words(_, Acc) ->
    random_words(4, Acc).

random_and_write(N) ->
    random_and_write(N, 0, 0).

random_and_write(N, Acc, Bcc) ->
    Level = 4,
    Words = random_words(Level, []),
    Topic = emqx_topic:join(Words),
    WordsTuple = list_to_tuple(Words),
    Msg = emqx_message:make(Topic, generate_random_binary(rand:uniform(20000) + 1000)),
    {T, _} = timer:tc(fun() ->
        mnesia:dirty_write(?TAB, #retained{topic = Topic, msg = Msg, expiry_time = 0, words = WordsTuple, level = size(WordsTuple)})
        end),
    case ets:info(?TAB, size) of
        N ->
            ?LOG(error, "dirty_write retainer create, time: ~w, random: ~w~n", [Bcc + T, Acc + 1]),
            ok;
        _ ->
            random_and_write(N, Acc + 1, Bcc + T)
    end.

write_to_trie('$end_of_table', Acc) ->
    Acc;
write_to_trie(Key, Acc) ->
    [#retained{topic = Topic, msg = Msg, expiry_time = ExpiryTime}] = ets:lookup(?TAB, Key),
    mnesia:async_dirty(fun() -> emqx_retainer_trie:insert(Topic, Msg, ExpiryTime) end),
    write_to_trie(ets:next(?TAB, Key), Acc + 1).

write_to_retainer('$end_of_table', Acc) ->
    Acc;
write_to_retainer(Key, Acc) ->
    [Retained] = ets:lookup(?TAB, Key),
    mnesia:dirty_write(?TAB, Retained),
    write_to_retainer(ets:next(?TAB, Key), Acc + 1).

prepare_test(N) ->
    rand:seed(exsplus),
    mnesia:clear_table(?TAB),
    mnesia:clear_table(emqx_retainer_trie_node),
    mnesia:clear_table(emqx_retainer_trie_edge),
    random_and_write(N),
    {T1, C1} = timer:tc(fun() -> write_to_retainer(ets:first(?TAB), 0) end),
    {T2, C2} = timer:tc(fun() -> write_to_trie(ets:first(?TAB), 0) end),
    {T3, C3} = timer:tc(fun() -> write_to_trie(ets:first(?TAB), 0) end),
    ?LOG(error, "dirty_write retainer update, time: ~w, count: ~w~n", [T1, C1]),
    ?LOG(error, "dirty_write trie create, time: ~w, count: ~w~n", [T2, C2]),
    ?LOG(error, "dirty_write trie update, time: ~w, count: ~w~n", [T3, C3]).

test_match() ->
    Topic = ets:first(?TAB),
    Words = emqx_topic:words(Topic),
    {Words1, _} = lists:foldr(fun
                             (_, {Acc, 1}) -> {['#'|Acc], 2};
                             (W, {Acc, N}) -> {[W|Acc], N + 1}
                         end, {[], 1}, Words),
    {Words2, _} = lists:foldr(fun
                             (_, {Acc, 2}) -> {['+'|Acc], 3};
                             (_, {Acc, 3}) -> {['+'|Acc], 4};
                             (W, {Acc, N}) -> {[W|Acc], N + 1}
                         end, {[], 1}, Words),
    Topic1 = emqx_topic:join(Words1),
    Topic2 = emqx_topic:join(Words2),
    Topic3 = <<"#">>,
    Topic4 = <<"+">>,
    Topic5 = <<"+/+">>,
    Topic6 = <<"$SYS/#">>,
    {Words7, _} = lists:foldr(fun
                             (_, {Acc, 1}) -> {Acc, 2};
                             (_, {Acc, 2}) -> {['#'|Acc], 3};
                             (W, {Acc, N}) -> {[W|Acc], N + 1}
                         end, {[], 1}, Words),
    Topic7 = emqx_topic:join(Words7),
    {Words8, _} = lists:foldr(fun
                                  (_, {Acc, 1}) -> {Acc, 2};
                                  (_, {Acc, 2}) -> {Acc, 3};
                                  (_, {Acc, 3}) -> {['#'|Acc], 4};
                                  (W, {Acc, N}) -> {[W|Acc], N + 1}
                              end, {[], 1}, Words),
    Topic8 = emqx_topic:join(Words8),
%%    test_match(Topic),
    test_match(Topic1),
    test_match(Topic2),
    test_match(Topic7),
    test_match(Topic8),
    test_match(Topic3),
    test_match(Topic4),
    test_match(Topic5),
    test_match(Topic6),
    ok.
test_match(Filter) ->
    {T1, L1} = timer:tc(fun() -> length(match_messages(Filter)) end),
    {T2, L2} = timer:tc(fun() -> length(match_messages_select(Filter)) end),
    {T3, L3} = timer:tc(fun() -> length(match_messages_trie(Filter)) end),
    ?LOG(error, "---- ~p ----~n", [Filter]),
    ?LOG(error, "foldl matching, time: ~w, topics: ~w~n", [T1, L1]),
    ?LOG(error, "select matching, time: ~w, topics: ~w~n", [T2, L2]),
    ?LOG(error, "trie matching, time: ~w, topics: ~w~n", [T3, L3]),
    ?LOG(error, "~n~n").

test_delete() ->
    Keys = mnesia:dirty_all_keys(?TAB),
    {T, _} = timer:tc(fun() -> mnesia:transaction(fun() -> [mnesia:delete(?TAB, K, write) || K<-Keys] end) end),
    ?LOG(error, "delete with transaction, time: ~w~n", [T]).


generate_random_binary(N) ->
    % The min packet length is 2
    Len = rand:uniform(N) + 1,
    gen_next(Len, <<>>).

gen_next(0, Acc) ->
    Acc;
gen_next(N, Acc) ->
    Byte = rand:uniform(256) - 1,
    gen_next(N-1, <<Acc/binary, Byte:8>>).