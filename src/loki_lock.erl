%%%-------------------------------------------------------------------
%% @doc loki lock
%% @end
%%%-------------------------------------------------------------------

-module(loki_lock).

-behaviour(gen_server).

-include("loki.hrl").

%% API
-export([start_link/0,
         new/0,
         delete/1,
         acquire/2,
         release/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
        gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Create a new lock table with optimized options
-spec new() -> {ok, ets:tid()}.
new() ->
    gen_server:call(?SERVER, new).

%% @doc Delete the lock table
-spec delete(ets:tid()) -> ok.
delete(Tid) ->
    true = ets:delete(Tid),
    ok.

%% @doc Acquire the lock for the given key
-spec acquire(ets:tid(), loki:key()) -> {ok, success} | {error, locked}.
acquire(Tid, Key) ->
    case ets:insert_new(Tid, {Key}) of
        true ->
            {ok, success};
        false ->
            {error, locked}
    end.

%% @doc Release existing lock for the given key
-spec release(ets:tid(), loki:key()) -> ok.
release(Tid, Key) ->
    true = ets:delete(Tid, Key),
    ok.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
        {ok, #state{}}.

handle_call(new, _From, State) ->
    %% ETS options
    %% - public: skip the gen_server for the writes
    %% - write_concurrency: handle lots of writes
    %% - read_concurrency: not enabled, makes it faster
    LockTable = ets:new(?MODULE, [public,
                                  {write_concurrency, true}]),
    {reply, {ok, LockTable}, State};

handle_call(_Request, _From, State) ->
        Reply = ok,
        {reply, Reply, State}.

handle_cast(_Msg, State) ->
        {noreply, State}.

handle_info(_Info, State) ->
        {noreply, State}.

terminate(_Reason, _State) ->
        ok.

code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
