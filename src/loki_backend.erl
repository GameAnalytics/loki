%%%-------------------------------------------------------------------
%% @doc loki
%% @end
%%%-------------------------------------------------------------------

-module(loki_backend).

-include("loki.hrl").

-callback start(loki:name(), list()) ->
    {ok, loki:ref()} | loki:error().

-callback stop(loki:loki()) ->
    ok.

-callback put(loki:loki(), loki:key(), loki:value()) ->
    ok.

-callback get(loki:loki(), loki:key()) ->
    {ok, loki:value()}.

-callback delete(loki:loki(), loki:key()) ->
    ok.

-callback update_fun(loki:loki(), loki:key(),
                     fun((loki:value()) -> loki:value())) ->
    ok | loki:error().

-callback update_fun(loki:loki(), loki:key(), loki:value(),
                     fun((loki:value(), loki:value()) -> loki:value())) ->
    ok | loki:error().
