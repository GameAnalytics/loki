-module(loki_backend_ets).

-include("loki.hrl").

-export([start/2,
         stop/1,
         destroy/2,
         put/3,
         get/2,
         delete/2,
         update/3,
         update_value/4,
         fold/3,
         from_list/2,
         to_list/1,
         checkpoint/3,
         from_checkpoint/3
        ]).

%% TODO Do we need a heir?

-spec start(loki:name(), list()) -> {ok, loki:ref()} | loki:error().
start(Name, Options) ->
    {ok, #backend{ref = ets:new(Name, [public,
                                       {read_concurrency, true},
                                       {write_concurrency, true}] ++ Options),
                  options = Options}}.

-spec stop(loki:backend()) -> ok.
stop(#backend{ref = Ref}) ->
    ets:delete(Ref),
    ok.

-spec destroy(loki:backend(), loki:name()) -> ok.
destroy(_Backend, _Name) ->
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
             fun((loki:key(), loki:value()) -> loki:value())) ->
    ok | loki:error().
update(Backend, Key, Fun) ->
    Value = case ?MODULE:get(Backend, Key) of
                {error, not_found} -> undefined;
                {ok, V}            -> V
            end,
    UpdatedValue = Fun(Key, Value),
    ?MODULE:put(Backend, Key, UpdatedValue).

-spec update_value(loki:backend(), loki:key(), loki:value(),
                   fun((loki:key(), loki:value(), loki:value()) -> loki:value())) ->
    ok | loki:error().
update_value(Backend, Key, NewValue, Fun) ->
    OldValue = case ?MODULE:get(Backend, Key) of
                   {error, not_found} -> undefined;
                   {ok, V}            -> V
               end,
    UpdatedValue = Fun(Key, OldValue, NewValue),
    ?MODULE:put(Backend, Key, UpdatedValue).

-spec fold(loki:backend(),
           fun((loki:key(), loki:value(), term()) -> term()), term()) -> term().
fold(#backend{ref = Ref}, Fun, AccIn) ->
    ets:foldl(fun({Key, Value}, Acc) ->
                      Fun(Key, Value, Acc)
              end, AccIn, Ref).

-spec from_list(loki:backend(), list({loki:key(), loki:value()})) ->
    ok | loki:error().
from_list(#backend{ref = Ref}, List) ->
    [ets:insert(Ref, {K, V}) || {K, V} <- List],
    ok.

-spec to_list(loki:backend()) -> list({loki:key(), loki:value()}).
to_list(#backend{ref = Ref}) ->
    ets:tab2list(Ref).

-spec checkpoint(loki:backend(), loki:name(), loki:path()) -> ok | loki:error().
checkpoint(#backend{ref = Ref} = Backend, Name, Path) ->
    FullPath = get_filename(Path, Name),
    ok = filelib:ensure_dir(FullPath),
    ets:tab2file(Ref, FullPath),
    {ok, Backend}.

-spec from_checkpoint(loki:name(), list(), loki:path()) ->
    {ok, loki:backend()} | loki:error().
from_checkpoint(Name, Options, Path) ->
    FullPath = get_filename(Path, Name),
    ok = filelib:ensure_dir(FullPath),
    {ok, Ref} = ets:file2tab(FullPath),
    {ok, #backend{ref = Ref,
                  options = Options}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_filename(Path, Name) ->
    filename:join([Path, Name]) ++ ".ets".
