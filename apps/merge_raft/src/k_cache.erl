%%% % @format

-module(k_cache).
-compile(warn_missing_spec_all).
-author("zeyu@meta.com").
-moduledoc """
A data structure that maintains k-th element of a set of key-ed values
""".

-export([
    new/1,
    size/1,
    member/2,
    get/1,
    update/2,
    delete/2
]).

-type type() :: min | max | mid_floor | mid_ceil | pos_integer() | fun((pos_integer()) -> pos_integer()).
-opaque k_cache(Key, Value) :: {type(), #{Key => Value}, gb_sets:set({Value, Key}), gb_sets:set({Value, Key})}.
-type k_cache() :: k_cache(_, _).

-export_type([
    type/0,
    k_cache/0,
    k_cache/2
]).

-spec new(type()) -> k_cache().
new(Type) ->
    {Type, #{}, gb_sets:new(), gb_sets:new()}.

-spec size(k_cache()) -> non_neg_integer().
size({_Type, Map, _L, _R}) ->
    map_size(Map).

-spec member(Key, k_cache(Key, _Value)) -> boolean().
member(Key, {_Type, Map, _L, _R}) ->
    is_map_key(Key, Map).

-spec get(k_cache(_Key, Value)) -> Value.
get({_Type, _Map, L, _R}) ->
    element(1, gb_sets:largest(L)).

-spec update(#{Key => Value}, k_cache(Key, Value)) -> k_cache(Key, Value).
update(Update, {Type, Map, L, R}) ->
    {L1, R1} = maps:fold(
        fun
            (Key, Value, {LAcc, RAcc}) ->
                case Map of
                    #{Key := Value} ->
                        {LAcc, RAcc};
                    #{Key := OldValue} ->
                        {LAcc1, RAcc1} =
                            case gb_sets:is_member({OldValue, Key}, LAcc) of
                                true ->
                                    {gb_sets:delete({OldValue, Key}, LAcc), RAcc};
                                _ ->
                                    {LAcc, gb_sets:delete({OldValue, Key}, RAcc)}
                            end,
                        case gb_sets:size(RAcc1) =:= 0 orelse {Value, Key} < gb_sets:smallest(RAcc1) of
                            true ->
                                {gb_sets:add({Value, Key}, LAcc1), RAcc1};
                            _ ->
                                {LAcc1, gb_sets:add({Value, Key}, RAcc1)}
                        end;
                    _ ->
                        case gb_sets:size(RAcc) =:= 0 orelse {Value, Key} < gb_sets:smallest(RAcc) of
                            true ->
                                {gb_sets:add({Value, Key}, LAcc), RAcc};
                            _ ->
                                {LAcc, gb_sets:add({Value, Key}, RAcc)}
                        end
                end
        end,
        {L, R},
        Update
    ),
    Map1 = maps:merge(Map, Update),
    {L2, R2} = resize(new_k(Type, map_size(Map1)), L1, R1),
    {Type, Map1, L2, R2}.

-spec delete([Key], k_cache(Key, Value)) -> k_cache(Key, Value).
delete(Delete, {Type, Map, L, R}) ->
    {L1, R1} = lists:foldl(
        fun(Key, {LAcc, RAcc}) ->
            case Map of
                #{Key := Value} ->
                    case gb_sets:is_member({Value, Key}, LAcc) of
                        true ->
                            {gb_sets:delete({Value, Key}, LAcc), RAcc};
                        _ ->
                            {LAcc, gb_sets:delete({Value, Key}, RAcc)}
                    end;
                _ ->
                    {LAcc, RAcc}
            end
        end,
        {L, R},
        Delete
    ),
    Map1 = maps:without(Delete, Map),
    {L2, R2} = resize(new_k(Type, map_size(Map1)), L1, R1),
    {Type, Map1, L2, R2}.

-spec new_k(type(), non_neg_integer()) -> non_neg_integer().
new_k(_Type, 0) ->
    0;
new_k(min, _N) ->
    1;
new_k(max, N) ->
    N;
new_k(mid_floor, N) ->
    (N + 1) div 2;
new_k(mid_ceil, N) ->
    (N div 2) + 1;
new_k(K, _N) when is_integer(K)->
    K;
new_k(Fun, N) when is_function(Fun) ->
    Fun(N).

-spec resize(non_neg_integer(), gb_sets:set({Value, Key}), gb_sets:set({Value, Key})) ->
    {gb_sets:set({Value, Key}), gb_sets:set({Value, Key})}.
resize(K, L, R) ->
    LSize = gb_sets:size(L),
    RSize = gb_sets:size(R),
    if
        LSize < K, RSize > 0 ->
            {Element, R1} = gb_sets:take_smallest(R),
            resize(K, gb_sets:add(Element, L), R1);
        LSize > K ->
            {Element, L1} = gb_sets:take_largest(L),
            resize(K, L1, gb_sets:add(Element, R));
        true ->
            {L, R}
    end.
