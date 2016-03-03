%%%-------------------------------------------------------------------
%% @doc loki
%% @end
%%%-------------------------------------------------------------------

-module(loki_backend).

-include("loki.hrl").

-callback start(loki:name(), list()) ->
    {ok, loki:backend()} | loki:error().

-callback stop(loki:backend()) ->
    ok.

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
