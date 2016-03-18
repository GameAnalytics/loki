-module(loki_backend_leveldb).

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
         checkpoint/3,
         from_checkpoint/3
        ]).

-spec start(loki:name(), list()) -> {ok, loki:ref()} | loki:error().
start(Name, Options) ->
    Path = db_path(Name, Options),
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),

    filelib:ensure_dir(filename:join(Path, "dummy")),

    case eleveldb:open(Path, DbOpts) of
        {ok, Ref} ->
            {ok, #backend{ref = Ref,
                          options = Options}};
        Error ->
            Error
    end.

-spec stop(loki:backend()) -> ok.
stop(#backend{ref = Ref}) ->
    eleveldb:close(Ref),
    ok.

-spec destroy(loki:backend(), loki:name()) -> ok.
destroy(#backend{options = Options}, Name) ->
    Path = db_path(Name, Options),
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),
    eleveldb:destroy(Path, DbOpts),
    file:del_dir(Path),
    ok.

-spec put(loki:backend(), loki:key(), loki:value()) -> ok.
put(#backend{ref = Ref}, Key, Value) ->
    eleveldb:put(Ref, enc(Key), enc(Value),
                 [{sync, true}]),
    ok.

-spec get(loki:backend(), loki:key()) -> {ok, loki:value()} | loki:error().
get(#backend{ref = Ref}, Key) ->
    case eleveldb:get(Ref, enc(Key), []) of
        {ok, Val} -> {ok, dec(Val)};
        not_found -> {error, not_found};
        Error -> Error
    end.

-spec delete(loki:backend(), loki:key()) -> ok.
delete(#backend{ref = Ref}, Key) ->
    eleveldb:delete(Ref, enc(Key), [{sync, true}]),
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
    eleveldb:fold(Ref, fun({Key, Value}, Acc) ->
                               Fun(dec(Key), dec(Value), Acc)
                       end, AccIn, []).

-spec fold_keys(loki:backend(),
                fun((loki:key(), term()) -> term()), term()) -> term().
fold_keys(#backend{ref = Ref}, Fun, AccIn) ->
    eleveldb:fold_keys(Ref, fun(Key, Acc) ->
                                    Fun(dec(Key), Acc)
                            end, AccIn, []).

-spec from_list(loki:backend(), list({loki:key(), loki:value()})) ->
    ok | loki:error().
from_list(#backend{ref = Ref}, List) ->
    Ops = [{put, enc(K), enc(V)} || {K, V} <- List],
    eleveldb:write(Ref, Ops, [{sync, true}]),
    ok.

-spec to_list(loki:backend()) -> list({loki:key(), loki:value()}).
to_list(Backend) ->
    fold(Backend, fun(Key, Value, Acc) -> [{Key, Value} | Acc] end, []).

%% eleveldb does not support live checkpoints/snapshots. We stop the DB, take
%% a copy of it and restart it as a workaround.
-spec checkpoint(loki:backend(), loki:name(), loki:path()) ->
    ok | loki:error().
checkpoint(#backend{options = Options} = Backend, Name, Path) ->
    ok = filelib:ensure_dir(Path),
    ok = stop(Backend),

    os:cmd("cd " ++ db_dir(Name, Options) ++
           "; tar -czf " ++ tar_file(Name) ++ " " ++ path(Name)),
    os:cmd("mv " ++ tar_file(Name) ++ " " ++ Path ++ "/"),

    start(Name, Options).

-spec from_checkpoint(loki:name(), list(), loki:path()) ->
    {ok, loki:backend()} | loki:error().
from_checkpoint(Name, Options, Path) ->
    DbPath = db_path(Name, Options),

    %% Ensure target directory is empty
    ec_file:remove(DbPath, [recursive]),

    %% Copy to target from backup
    ok = filelib:ensure_dir(DbPath),
    os:cmd("cd " ++ Path ++
           "; tar -xzf " ++ tar_file(Name) ++ " -C " ++ current_full_path()),

    start(Name, Options).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

db_dir(Name, Options) ->
    filename:dirname(db_path(Name, Options)).

db_path(Name, Options) ->
    case proplists:get_value(db_dir, Options) of
        undefined -> path(Name);
        Dir       -> full_path(Dir, Name)
    end.

enc(Object) ->
    term_to_binary(Object).

dec(Object) ->
    binary_to_term(Object).

path(Name) ->
    erlang:atom_to_list(Name).

full_path(Path, Name) ->
    filename:join([Path, Name]).

tar_file(Name) when is_atom(Name) ->
    path(Name) ++ ".tar.gz".

current_full_path() ->
    filename:absname("").
