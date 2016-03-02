-module(loki_backend_ets).

-include("loki.hrl").

-export([start/2,
         stop/1,
         put/3,
         get/2,
         delete/2,
         update_fun/3, update_fun/4
        ]).

%% TODO Do we need a heir?

-spec start(loki:name(), list()) -> {ok, loki:ref()}.
start(Name, Config) ->
    {ok, ets:new(Name, Config)}.

-spec stop(loki:loki()) -> ok.
stop(Store) ->
    ets:delete(Store#store.ref),
    ok.

-spec put(loki:loki(), loki:key(), loki:value()) -> ok.
put(Store, Key, Value) ->
    true = ets:insert(Store#store.ref, {Key, Value}),
    ok.

-spec get(loki:loki(), loki:key()) -> {ok, loki:value()} | loki:error().
get(Store, Key) ->
    case ets:lookup(Store#store.ref, Key) of
        [] ->
            {error, not_found};
        [{Key, Value}] ->
            {ok, Value}
    end.

-spec delete(loki:loki(), loki:key()) -> ok.
delete(Store, Key) ->
    true = ets:delete(Store#store.ref, Key),
    ok.

-spec update_fun(loki:loki(), loki:key(),
                     fun((loki:value()) -> loki:value())) ->
    ok | loki:error().

update_fun(Store, Key, Fun) ->
    Value = case ?MODULE:get(Store, Key) of
                {error, not_found} -> undefined;
                {ok, V}            -> V
            end,
    UpdatedValue = Fun(Value),
    ?MODULE:put(Store, Key, UpdatedValue).

-spec update_fun(loki:loki(), loki:key(), loki:value(),
                 fun((loki:value(), loki:value()) -> loki:value())) ->
    ok | loki:error().
update_fun(Store, Key, NewValue, Fun) ->
    OldValue = case ?MODULE:get(Store, Key) of
                   {error, not_found} -> undefined;
                   {ok, V}            -> V
               end,
    UpdatedValue = Fun(OldValue, NewValue),
    ?MODULE:put(Store, Key, UpdatedValue).
