%%%-------------------------------------------------------------------
%% @doc loki
%%
%% Key value store with configurable backend and locks
%%
%% Loki in a wrapper around different key value backends with provides the
%% feature that all updates to the key value store are atomic and exclusive at
%% the key level. It is implemented using an ets table for locks.
%% @end
%%%-------------------------------------------------------------------

-module(loki).

-include("loki.hrl").

-export([start/3,
         stop/1,
         destroy/1,
         put/3, put/4,
         get/2,
         delete/2,
         update/3, update/4,
         update_value/4, update_value/5,
         fold/3,
         from_list/2,
         to_list/1,
         checkpoint/2,
         from_checkpoint/4
        ]).

-define(DEFAULT_BACKEND, loki_backend_ets).

-define(DEFAULT_TIMEOUT, infinity).

-type store() :: #store{}.
-type backend() :: #backend{}.
-type name() :: atom().
-type ref() :: term(). %% TODO list out all specific types of backend returns
-type key() :: term().
-type value() :: term().
-type error() :: {error, term()}.
-type path() :: string().

-export_type([store/0,
              name/0,
              backend/0,
              ref/0,
              key/0,
              value/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a new instance of loki with specified backend (ets backend by
%% default).
%% TODO create type for config and options
%% TODO Implement unique names? Is it necessary? (Will need a manager for it)
-spec start(name(), list(), list()) -> {ok, store()} | error().
start(Name, Config, Options) ->
    Mod = proplists:get_value(backend, Options, ?DEFAULT_BACKEND),
    {ok, Backend} = Mod:start(Name, Config),
    do_start(Name, Options, Mod, Backend).

%% @doc Stop the store
-spec stop(store()) -> ok | error().
stop(#store{mod = Mod, lock_table = LockTable, backend = Backend}) ->
    ok = Mod:stop(Backend),
    ok = loki_lock:delete(LockTable).

%% @doc Delete any file based key-value backend
-spec destroy(store()) -> ok | error().
destroy(#store{mod = Mod, backend = Backend, name = Name} = Store) ->
    ok = stop(Store),
    ok = Mod:destroy(Backend, Name).

%% @doc Put key value into store. Overwrites existing value.
-spec put(store(), key(), value()) -> ok | error().
put(Store, Key, Value) ->
    put(Store, Key, Value, ?DEFAULT_TIMEOUT).

%% @doc @see put/3 with timeout
-spec put(store(), key(), value(), timeout()) -> ok | error().
put(#store{mod = Mod, lock_table = LockTable, backend = Backend},
    Key, Value, Timeout) ->
    lock_exec(LockTable, Key,
              fun() -> ok = Mod:put(Backend, Key, Value) end,
              Timeout).

%% @doc Get value for given key
-spec get(store(), key()) -> {ok, value()} | error().
get(#store{mod = Mod, backend = Backend}, Key) ->
    Mod:get(Backend, Key).

%% @doc Delete a key value pair specified by the given key
-spec delete(store(), key()) -> ok | error().
delete(#store{mod = Mod, lock_table = LockTable, backend = Backend}, Key) ->
    lock_exec(LockTable, Key,
              fun() -> ok = Mod:delete(Backend, Key) end).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives the current value indexed by the key.
-spec update(store(), key(), fun((key(), value()) -> value())) -> ok | error().
update(Store, Key, Fun) ->
    update(Store, Key, Fun, ?DEFAULT_TIMEOUT).

%% @doc @see update/3 with timeout.
-spec update(store(), key(), fun((key(), value()) -> value()), timeout()) ->
    ok | error().
update(#store{mod = Mod, lock_table = LockTable, backend = Backend}, Key, Fun,
       Timeout) ->
    lock_exec(LockTable, Key,
              fun() -> Mod:update(Backend, Key, Fun) end,
              Timeout).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives both, the existing value indexed by key and new value
%% passed to it externally.
-spec update_value(store(), key(), value(),
                   fun((key(), value(), value()) -> value())) -> ok | error().
update_value(Store, Key, Value, Fun) ->
    update_value(Store, Key, Value, Fun, ?DEFAULT_TIMEOUT).

%% @doc @see update_value/4 with timeout.
-spec update_value(store(), key(), value(),
                   fun((key(), value(), value()) -> value()), timeout()) ->
    ok | error().
update_value(#store{mod = Mod, lock_table = LockTable, backend = Backend},
             Key, Value, Fun, Timeout) ->
    lock_exec(LockTable, Key,
              fun() -> Mod:update_value(Backend, Key, Value, Fun) end,
              Timeout).

%% @doc Fold over all key value pairs
-spec fold(store(), fun((key(), value(), term()) -> term()), term()) -> term().
fold(#store{mod = Mod, backend = Backend}, Fun, Acc) ->
    Mod:fold(Backend, Fun, Acc).

%% @doc Insert key value pairs into loki from the given list
-spec from_list(store(), list({key(), value()})) -> ok.
from_list(#store{mod = Mod, backend = Backend}, List) ->
    Mod:from_list(Backend, List).

%% @doc Return all key value pairs as list
-spec to_list(store()) -> list({key(), value()}).
to_list(#store{mod = Mod, backend = Backend}) ->
    Mod:to_list(Backend).

%% @doc Create a complete backup of the database at the given absolute path
-spec checkpoint(store(), path()) -> {ok, store()} | error().
checkpoint(#store{mod = Mod, backend = Backend, name = Name} = Store, Path) ->
     case Mod:checkpoint(Backend, Name, Path) of
         {ok, NewBackend} ->
             {ok, Store#store{backend = NewBackend}};
         Error ->
             Error
     end.

%% @doc Start a new instance of loki with specified backend (ets backend by
%% default) from a given checkpoint
-spec from_checkpoint(name(), list(), list(), path()) -> {ok, store()} | error().
from_checkpoint(Name, Config, Options, Path) ->
    Mod = proplists:get_value(backend, Options, ?DEFAULT_BACKEND),
    {ok, Backend} = Mod:from_checkpoint(Name, Config, Path),
    do_start(Name, Options, Mod, Backend).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Acquire lock and execute given function, error if lock cannot be acquired
lock_exec(LockTable, Key, Fun) ->
    case loki_lock:acquire(LockTable, Key) of
        {ok, success} ->
            Result = Fun(),
            loki_lock:release(LockTable, Key),
            Result;
        {error, locked} ->
            {error, locked}
    end.

%% Same as lock_exec/3 but use erlang:yield/0 to release the scheduler and try
%% again either for ever (infinity) or immediately (0) or given interval.
%% Note: erlang:yield/0 is equivalent to sleeping for 1ms.
lock_exec(LockTable, Key, Fun, infinity = Timeout) ->
    case lock_exec(LockTable, Key, Fun) of
        {error, locked} ->
            true = erlang:yield(),
            lock_exec(LockTable, Key, Fun, Timeout);
        Result ->
            Result
    end;
lock_exec(LockTable, Key, Fun, 0) ->
    lock_exec(LockTable, Key, Fun);
lock_exec(LockTable, Key, Fun, Timeout) ->
    Start = os:timestamp(),
    lock_exec(LockTable, Key, Fun, Start, Timeout).

lock_exec(LockTable, Key, Fun, Start, Timeout) ->
    case (timer:now_diff(os:timestamp(), Start) div 1000) >= Timeout of
        true ->
            {error, timeout};
        false ->
            case lock_exec(LockTable, Key, Fun) of
                {error, locked} ->
                    true = erlang:yield(),
                    lock_exec(LockTable, Key, Fun, Start, Timeout);
                Result ->
                    Result
            end
    end.

do_start(Name, Options, Mod, Backend) ->
    {ok, LockTable} = loki_lock:new(),
    {ok, #store{name = Name,
                mod = Mod,
                backend = Backend,
                lock_table = LockTable,
                options = Options}}.
