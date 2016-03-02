-module(loki_test).

-include_lib("eunit/include/eunit.hrl").


loki_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [
      {"Simple run",       fun simple_run/0}
     ]}.

setup() ->
    application:start(loki).

teardown(_) ->
    application:stop(loki).

simple_run() ->
    {ok, Store} = loki:start(test_kv, [], []),
    Key = key,
    Value = 1,

    ok = loki:put(Store, Key, Value),
    {ok, ReceivedValue1} = loki:get(Store, Key),

    ?assertEqual(Value, ReceivedValue1),
    ?assertEqual({error, not_found}, loki:get(Store, unknown_key)),

    ok = loki:update_fun(Store, Key, fun(V) -> V + 1 end),
    {ok, ReceivedValue2} = loki:get(Store, Key),
    ?assertEqual(Value + 1, ReceivedValue2),

    ok = loki:update_fun(Store, Key, 100, fun(OldV, NewV) -> OldV + NewV  end),
    {ok, ReceivedValue3} = loki:get(Store, Key),
    ?assertEqual(Value + 101, ReceivedValue3),

    ok = loki:stop(Store).
