%%%-------------------------------------------------------------------
%% @doc loki
%% @end
%%%-------------------------------------------------------------------

-module(loki).

-include("loki.hrl").

-export([]).

-define(DEFAULT_BACKEND, loki_ets).

-type loki() :: #store{}.
-type reason() :: term().
-type key() :: term().
-type value() :: term().

-export_type([loki/0,
              key/0,
              value/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a new instance of loki with specified backend (ets backend by
%% default).
-spec start(binary(), list()) -> {ok, loki()} | {error, reason()}.
start(Name, Options) ->
    Backend = proplists:get_value(backend, Options, ?DEFAULT_BACKEND),
    Backend:start(Name, Options).

%% @doc Stop the store
-spec stop(loki()) -> ok | {error, reason()}.
stop(#store{backend = Backend} = Store) ->
    Backend:stop(Store).

%% @doc Put key value into store
-spec put(loki(), key(), value()) -> {ok, loki()} | {error, reason()}.
put(#store{backend = Backend} = Store, Key, Value) ->
    Backend:put(Store, Key, Value).

%% @doc Get value for given key
-spec get(loki(), key()) -> {ok, value()} | {error, reason()}.
get(#store{backend = Backend} = Store, Key) ->
    Backend:get(Store, Key).

%% @doc Delete a key value pair specified by the given key
-spec delete(loki(), key()) -> ok | {error, reason()}.
delete(#store{backend = Backend} = Store, Key) ->
    Backend:delete(Store, Key).

%% @doc Update given key with new value (overwrites existing value)
-spec update(loki(), key(), value()) -> {ok, loki()} | {error, reason()}.
update(#store{backend = Backend} = Store, Key, Value) ->
    Backend:update(Store, Key, Value).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives the current value indexed by the key.
-spec update_fun(loki(), key(), fun((value()) -> value())) ->
    {ok, loki()} | {error, reason()}.
update_fun(#store{backend = Backend} = Store, Key, Fun) ->
    Backend:update_fun(Store, Key, Fun).

%% @doc Update given key with new value obtained by calling given function.
%% The function receives both, the existing value indexed by key and new value
%% passed to it externally.
-spec update_fun(loki(), key(), value(), fun((value(), value()) -> value())) ->
    {ok, loki()} | {error, reason()}.
update_fun(#store{backend = Backend} = Store, Key, Value, Fun) ->
    Backend:update_fun(Store, Key, Value, Fun).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
