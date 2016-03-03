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
         put/3,
         get/2,
         delete/2,
         update_fun/3, update_fun/4
        ]).

-define(DEFAULT_BACKEND, loki_backend_ets).

-type loki() :: #store{}.
-type name() :: atom().
-type ref() :: term(). %% TODO list out all specific types of backend returns
-type key() :: term().
-type value() :: term().
-type error() :: {error, term()}.

-export_type([loki/0,
              name/0,
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
-spec start(name(), list(), list()) -> {ok, loki()}.
start(Name, Config, Options) ->
    Backend = proplists:get_value(backend, Options, ?DEFAULT_BACKEND),
    {ok, LockTable} = loki_lock:new(),
    %% TODO check return values
    {ok, Ref} = Backend:start(Name, Config),
    {ok, #store{name = Name,
                backend = Backend,
                ref = Ref,
                lock_table = LockTable,
                config = Config,
                options = Options}}.

%% @doc Stop the store
-spec stop(loki()) -> ok | error().
stop(#store{backend = Backend} = Store) ->
    ok = Backend:stop(Store),
    ok = loki_lock:delete(Store#store.lock_table).

%% @doc Put key value into store. Overwrites existing value.
-spec put(loki(), key(), value()) -> ok | error().
put(#store{backend = Backend} = Store, Key, Value) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> ok = Backend:put(Store, Key, Value) end).

%% @doc Get value for given key
%% TODO Do we need locks for reads?
-spec get(loki(), key()) -> {ok, value()} | error().
get(#store{backend = Backend} = Store, Key) ->
    Backend:get(Store, Key).

%% @doc Delete a key value pair specified by the given key
-spec delete(loki(), key()) -> ok | error().
delete(#store{backend = Backend} = Store, Key) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> ok = Backend:delete(Store, Key) end).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives the current value indexed by the key.
-spec update_fun(loki(), key(),
                 fun((key(), value()) -> value())) -> ok | error().
update_fun(#store{backend = Backend} = Store, Key, Fun) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> Backend:update_fun(Store, Key, Fun) end).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives both, the existing value indexed by key and new value
%% passed to it externally.
-spec update_fun(loki(), key(), value(),
                 fun((key(), value(), value()) -> value())) -> ok | error().
update_fun(#store{backend = Backend} = Store, Key, Value, Fun) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> Backend:update_fun(Store, Key, Value, Fun) end).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lock_exec(LockTable, Key, Fun) ->
    case loki_lock:acquire(LockTable, Key) of
        {ok, success} ->
            Result = Fun(),
            loki_lock:release(LockTable, Key),
            Result;
        {error, locked} = Error ->
            Error
    end.
