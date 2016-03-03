-module(loki_backend_ets).

-include("loki.hrl").

-export([start/2,
         stop/1,
         put/3,
         get/2,
         delete/2,
         update/3,
         update_value/4
        ]).

%% TODO Do we need a heir?

-spec start(loki:name(), list()) -> {ok, loki:ref()}.
start(Name, Options) ->
    {ok, #backend{ref = ets:new(Name, Options),
                  options = Options}}.

-spec stop(loki:backend()) -> ok.
stop(#backend{ref = Ref}) ->
    ets:delete(Ref),
    ok.

-spec put(loki:backend(), loki:key(), loki:value()) -> ok.
put(#backend{ref = Ref}, Key, Value) ->
    true = ets:insert(Ref, {Key, Value}),
    ok.

-spec get(loki:backend(), loki:key()) -> {ok, loki:value()} | loki:error().
get(#backend{ref = Ref}, Key) ->
    case ets:lookup(Ref, Key) of
        [] ->
            {error, not_found};
        [{Key, Value}] ->
            {ok, Value}
    end.

-spec delete(loki:backend(), loki:key()) -> ok.
delete(#backend{ref = Ref}, Key) ->
    true = ets:delete(Ref, Key),
    ok.

-spec update(loki:backend(), loki:key(),
             fun((loki:value()) -> loki:value())) ->
    ok | loki:error().
update(Backend, Key, Fun) ->
    Value = case ?MODULE:get(Backend, Key) of
                {error, not_found} -> undefined;
                {ok, V}            -> V
            end,
    UpdatedValue = Fun(Key, Value),
    ?MODULE:put(Backend, Key, UpdatedValue).

-spec update_value(loki:backend(), loki:key(), loki:value(),
                   fun((loki:value(), loki:value()) -> loki:value())) ->
    ok | loki:error().
update_value(Backend, Key, NewValue, Fun) ->
    OldValue = case ?MODULE:get(Backend, Key) of
                   {error, not_found} -> undefined;
                   {ok, V}            -> V
               end,
    UpdatedValue = Fun(Key, OldValue, NewValue),
    ?MODULE:put(Backend, Key, UpdatedValue).
