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
         put/3, put/4,
         get/2,
         delete/2,
         update/3, update/4,
         update_value/4, update_value/5
        ]).

-define(DEFAULT_BACKEND, loki_backend_ets).

-define(DEFAULT_TIMEOUT, infinity).

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
put(Store, Key, Value) ->
    put(Store, Key, Value, ?DEFAULT_TIMEOUT).

%% @doc @see put/3 with timeout
-spec put(loki(), key(), value(), timeout()) -> ok | error().
put(#store{backend = Backend} = Store, Key, Value, Timeout) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> ok = Backend:put(Store, Key, Value) end,
              Timeout).

%% @doc Get value for given key
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
-spec update(loki(), key(), fun((key(), value()) -> value())) -> ok | error().
update(Store, Key, Fun) ->
    update(Store, Key, Fun, ?DEFAULT_TIMEOUT).

%% @doc @see update/3 with timeout.
-spec update(loki(), key(), fun((key(), value()) -> value()), timeout()) ->
    ok | error().
update(#store{backend = Backend} = Store, Key, Fun, Timeout) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> Backend:update(Store, Key, Fun) end,
              Timeout).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives both, the existing value indexed by key and new value
%% passed to it externally.
-spec update_value(loki(), key(), value(),
                   fun((key(), value(), value()) -> value())) -> ok | error().
update_value(Store, Key, Value, Fun) ->
    update_value(Store, Key, Value, Fun, ?DEFAULT_TIMEOUT).

%% @doc @see update_value/4 with timeout.
-spec update_value(loki(), key(), value(),
                 fun((key(), value(), value()) -> value()), timeout()) ->
    ok | error().
update_value(#store{backend = Backend} = Store, Key, Value, Fun, Timeout) ->
    lock_exec(Store#store.lock_table, Key,
              fun() -> Backend:update_value(Store, Key, Value, Fun) end,
              Timeout).

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
