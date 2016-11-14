-module(loki_test).

-include_lib("eunit/include/eunit.hrl").

loki_test_() ->
    {setup, fun setup/0, fun teardown/1,
     [
      {"Simple run",             fun simple_run/0},
      {"Fold",                   fun fold/0},
      {"Reduce 100/1",           ?_test(reduce(100, 1))},
      {"Reduce 100/10",          ?_test(reduce(100, 10))},
      {"Reduce 100/13",          ?_test(reduce(100, 13))},
      {"Reduce 100/50",          ?_test(reduce(100, 50))},
      {"Reduce 100/100",         ?_test(reduce(100, 100))},
      {"Reduce 10/200",          ?_test(reduce(10,  200))},
      {"Reduce 0/1",             ?_test(reduce(0,   1))},
      {"List import export",     fun list_import_export/0},
      {"Checkpoint and restore", fun checkpoint_restore/0}
     ]}.

-define(OPTIONS, [{backend, loki_backend_ets},
                  {hash_locks, true}]).

setup() ->
    application:start(loki).

teardown(_) ->
    application:stop(loki).

simple_run() ->
    {ok, Store} = loki:start(test_kv, [], ?OPTIONS),
    Key = key,
    Value = 1,

    ok = loki:put(Store, Key, Value),
    {ok, ReceivedValue1} = loki:get(Store, Key),

    ?assertEqual(Value, ReceivedValue1),
    ?assertEqual({error, not_found}, loki:get(Store, unknown_key)),

    ok = loki:update(Store, Key, fun(_K, V) -> V + 1 end),
    {ok, ReceivedValue2} = loki:get(Store, Key),
    ?assertEqual(Value + 1, ReceivedValue2),

    ok = loki:update_value(Store, Key, 100, fun(_K, NewV, OldV) -> OldV + NewV  end),
    {ok, ReceivedValue3} = loki:get(Store, Key),
    ?assertEqual(Value + 101, ReceivedValue3),

    ok = loki:destroy(Store).

fold() ->
    {ok, Store} = loki:start(test_kv, [], ?OPTIONS),

    [loki:put(Store, E, E) || E <- lists:seq(1, 100)],

    Sum = loki:fold(Store, fun(_K, V, Acc) -> Acc + V end, 0),

    ?assertEqual(100 * 101 div 2, Sum),

    SumKeys = loki:fold_keys(Store, fun(K, Acc) -> Acc + K end, 0),

    ?assertEqual(100 * 101 div 2, SumKeys),

    ok = loki:destroy(Store).

reduce(DataSize, BatchSize) ->

    {ok, Store} = loki:start(test_kv, [], ?OPTIONS),

    [loki:put(Store, E, 1) || E <- lists:seq(1, DataSize)],

    ReduceResult =
        loki:reduce(Store,
            fun (Batch) ->
                {Keys, Values} = lists:unzip(Batch),
                {lists:sum(Keys), lists:sum(Values)}
            end, BatchSize),

    {KeySubTotal, ValueSubTotal} = lists:unzip(ReduceResult),

    ?assertEqual((DataSize + 1) * DataSize div 2, lists:sum(KeySubTotal)),
    ?assertEqual(DataSize, lists:sum(ValueSubTotal)),
    case DataSize rem BatchSize of
        0 -> ?assertEqual(DataSize div BatchSize, length(ReduceResult)),
             ?assert(lists:all(fun (N) -> N =:= BatchSize end, ValueSubTotal));
        R -> ?assertEqual(DataSize div BatchSize + 1, length(ReduceResult)),
             ?assert(lists:all(fun (N) -> N =:= BatchSize end, tl(ValueSubTotal))),
             ?assertEqual(R, hd(ValueSubTotal))
    end,

    ok = loki:destroy(Store).

list_import_export() ->
    {ok, Store} = loki:start(test_kv, [], ?OPTIONS),

    Elements = lists:seq(1, 100),
    List = [{E, E} || E <- Elements],

    ok = loki:from_list(Store, List),

    ResultList = loki:to_list(Store),

    ?assertEqual(List, lists:sort(ResultList)),

    ?assertEqual(Elements, lists:sort(loki:keys(Store))),

    ok = loki:destroy(Store).

checkpoint_restore() ->
    Name = test_kv,
    {ok, Store0} = loki:start(Name, [], ?OPTIONS),

    List = [{E, E} || E <- lists:seq(1, 100)],

    ok = loki:from_list(Store0, List),

    Path = "/tmp/loki/",
    ec_file:remove(Path ++ loki_util:to_list(Name), [recursive]),

    {ok, NewStore0} = loki:checkpoint(Store0, Path),
    ok = loki:destroy(NewStore0),

    {ok, Store1} = loki:from_checkpoint(Name, [], ?OPTIONS, Path),

    Result = loki:to_list(Store1),
    ?assertEqual(List, lists:sort(Result)),

    ok = loki:destroy(Store1).
