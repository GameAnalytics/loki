%%%-------------------------------------------------------------------
%% @doc loki
%% @end
%%%-------------------------------------------------------------------

-module(loki_backend).

-include("loki.hrl").

-callback start(loki:name(), list()) ->
    {ok, loki:backend()} | loki:error().

-callback stop(loki:backend()) ->
    ok | loki:error().

-callback destroy(loki:backend(), loki:name()) ->
    ok | loki:error().

-callback put(loki:backend(), loki:key(), loki:value()) ->
    ok.

-callback get(loki:backend(), loki:key()) ->
    {ok, loki:value()}.

-callback delete(loki:backend(), loki:key()) ->
    ok.

-callback update(loki:backend(), loki:key(),
                 fun((loki:key(), loki:value()) -> loki:value())) ->
    ok | loki:error().

-callback update_value(loki:backend(), loki:key(), loki:value(),
                       fun((loki:key(), loki:value(), loki:value()) ->
                       loki:value())) ->
    ok | loki:error().

-callback fold(loki:backend(),
               fun((loki:key(), loki:value(), term()) -> term()), term()) ->
    term().

-callback fold_keys(loki:backend(),
                    fun((loki:key(), term()) -> term()), term()) ->
    term().

-callback from_list(loki:backend(), list({loki:key(), loki:value()})) ->
    ok.

-callback to_list(loki:backend()) ->
    list({loki:key(), loki:value()}).

-callback keys(loki:backend()) ->
    list(loki:key()).

-callback checkpoint_name(loki:name()) ->
    string().

-callback checkpoint(loki:backend(), loki:name(), loki:path()) ->
    {ok, loki:backend()} | loki:error().

-callback from_checkpoint(loki:name(), list(), loki:path()) ->
    {ok, loki:backend()} | loki:error().

-callback status(loki:backend()) ->
    term().

-callback status(loki:backend(), term()) ->
    term().
