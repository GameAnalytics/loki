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
         fold_keys/3,
         from_list/2,
         to_list/1,
         checkpoint_name/1,
         checkpoint/3,
         from_checkpoint/3
        ]).

-spec start(loki:name(), list()) -> {ok, loki:ref()} | loki:error().
start(Name, Options) ->
    Path = db_path(Name, Options),
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
    Path = db_path(Name, Options),
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),
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
    UpdatedValue = Fun(Key, NewValue, OldValue),
    ?MODULE:put(Backend, Key, UpdatedValue).

-spec fold(loki:backend(),
           fun((loki:key(), loki:value(), term()) -> term()), term()) -> term().
fold(#backend{ref = Ref}, Fun, AccIn) ->
    erocksdb:fold(Ref, fun({Key, Value}, Acc) ->
                                Fun(dec(Key), dec(Value), Acc)
                        end, AccIn, []).

-spec fold_keys(loki:backend(),
                fun((loki:key(), term()) -> term()), term()) -> term().
fold_keys(#backend{ref = Ref}, Fun, AccIn) ->
    erocksdb:fold_keys(Ref, fun(Key, Acc) ->
                                    Fun(dec(Key), Acc)
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

-spec checkpoint_name(loki:name()) -> string().
checkpoint_name(Name) ->
    tar_file(loki_util:to_list(Name)).

-spec checkpoint(loki:backend(), loki:name(), loki:path()) ->
    ok | loki:error().
checkpoint(#backend{ref = Ref} = Backend, Name, Path) ->
    NameStr= loki_util:to_list(Name),
    FullPath = full_path(Path, NameStr),
    ok = filelib:ensure_dir(Path),
    ok = erocksdb:checkpoint(Ref, FullPath),
    os:cmd("cd " ++ Path ++
           "; tar -czf " ++ tar_file(NameStr) ++ " " ++ NameStr),
    ec_file:remove(FullPath, [recursive]),
    {ok, Backend}.

-spec from_checkpoint(loki:name(), list(), loki:path()) ->
    {ok, loki:backend()} | loki:error().
from_checkpoint(Name, Options, Path) ->
    NameStr = loki_util:to_list(Name),
    DbPath = db_path(Name, Options),

    %% Ensure target directory is empty
    ec_file:remove(DbPath, [recursive]),

    %% Copy to target from backup
    ok = filelib:ensure_dir(DbPath),
    os:cmd("cd " ++ Path ++
           "; tar -xzf " ++ tar_file(NameStr) ++ " -C " ++ current_full_path()),
    start(Name, Options).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

db_path(Name, Options) ->
    NameStr = loki_util:to_list(Name),
    case proplists:get_value(db_dir, Options) of
        undefined -> NameStr;
        Dir       -> full_path(Dir, NameStr)
    end.

enc(Object) ->
    term_to_binary(Object).

dec(Object) ->
    binary_to_term(Object).

full_path(Path, Name) ->
    filename:join([Path, Name]).

tar_file(NameStr) ->
    NameStr ++ ".tar.gz".

current_full_path() ->
    filename:absname("").
