-module(loki_backend_rocksdb).

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
         to_list/1
        ]).

-spec start(loki:name(), list()) -> {ok, loki:ref()} | loki:error().
start(Name, Options) ->
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> path(Name);
               Dir       -> filename:join(Dir, Name)
           end,
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),

    filelib:ensure_dir(filename:join(Path, "dummy")),

    case erocksdb:open(Path, DbOpts, []) of
        {ok, Ref} ->
            {ok, #backend{ref = Ref,
                          options = Options}};
        Error ->
            Error
    end.

-spec stop(loki:backend()) -> ok.
stop(#backend{ref = Ref}) ->
    erocksdb:close(Ref),
    ok.

-spec destroy(loki:backend(), loki:name()) -> ok.
destroy(#backend{options = Options}, Name) ->
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> path(Name);
               Dir       -> filename:join(Dir, Name)
           end,
    erocksdb:destroy(Path, DbOpts),
    file:del_dir(Path),
    ok.

-spec put(loki:backend(), loki:key(), loki:value()) -> ok.
put(#backend{ref = Ref}, Key, Value) ->
    erocksdb:put(Ref, enc(Key), enc(Value),
                 [{sync, true}]),
    ok.

-spec get(loki:backend(), loki:key()) -> {ok, loki:value()} | loki:error().
get(#backend{ref = Ref}, Key) ->
    case erocksdb:get(Ref, enc(Key), []) of
        {ok, Val} -> {ok, dec(Val)};
        not_found -> {error, not_found};
        Error -> Error
    end.

-spec delete(loki:backend(), loki:key()) -> ok.
delete(#backend{ref = Ref}, Key) ->
    erocksdb:delete(Ref, enc(Key), [{sync, true}]),
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

-spec fold(loki:backend(),
           fun((loki:key(), loki:value(), term()) -> term()), term()) -> term().
fold(#backend{ref = Ref}, Fun, AccIn) ->
    erocksdb:fold(Ref, fun({Key, Value}, Acc) ->
                                Fun(dec(Key), dec(Value), Acc)
                        end, AccIn, []).

-spec from_list(loki:backend(), list({loki:key(), loki:value()})) ->
    ok | loki:error().
from_list(#backend{ref = Ref}, List) ->
    Ops = [{put, enc(K), enc(V)} || {K, V} <- List],
    erocksdb:write(Ref, Ops, [{sync, true}]),
    ok.

-spec to_list(loki:backend()) -> list({loki:key(), loki:value()}).
to_list(Backend) ->
    fold(Backend, fun(Key, Value, Acc) -> [{Key, Value} | Acc] end, []).

enc(Object) ->
    term_to_binary(Object).

dec(Object) ->
    binary_to_term(Object).

path(Name) ->
    erlang:atom_to_list(Name).
