-module(emysql_orm).
-include_lib("kernel/include/logger.hrl").
-include_lib("emysql/include/emysql.hrl").
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  i/0,
  add_cfg/6,
  get_default_database/0
]).

-export([
  find/2,
  find/3,
  find/5
]).

-export([
  count/3,
  insert/3,
  replace/3,
  update/4,
  update/6,
  delete/3,
  delete/5
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).
-define(EMYSQL_ORM_CFG_ETS, emysql_cfg_ets).

-define(COL_TYPE_BOOL, boolean).
-define(COL_TYPE_INTEGER, integer).
-define(COL_TYPE_STRING, string).
-define(COL_TYPE_DATETIME, datetime).
-define(COL_TYPE_DATE, date).
-define(COL_TYPE_BINARY, binary).
-define(COL_TYPE_TERM, term).% 将数据以 blob  格式存在数据库
-define(COL_TYPE_ALL, [
  ?COL_TYPE_BOOL,
  ?COL_TYPE_INTEGER,
  ?COL_TYPE_STRING,
  ?COL_TYPE_DATETIME,
  ?COL_TYPE_DATE,
  ?COL_TYPE_BINARY,
  ?COL_TYPE_TERM
]).

-define(COND_OP_LT, lt).
-define(COND_OP_LTE, lte).
-define(COND_OP_EQ, eq).
-define(COND_OP_GTE, gte).
-define(COND_OP_GT, gt).
-define(COND_OP_LIKE, like).
-define(COND_OP_NEQ, neq).
-define(COND_OP_LIST, [
  ?COND_OP_LT,
  ?COND_OP_LTE,
  ?COND_OP_EQ,
  ?COND_OP_GTE,
  ?COND_OP_GT,
  ?COND_OP_LIKE,
  ?COND_OP_NEQ
]).

-define(ORDER_TYPE_ASC, asc).
-define(ORDER_TYPE_DESC, desc).
-define(DEF_TIMEOUT, 1000 * 10).
-record(state, {}).

-record(record_cfg, {
  key :: atom(),
  db_name :: binary(),
  table_name :: binary(),
  share_func :: {M :: atom(), F :: atom()} | undefined, % Share(Param) -> Pool|{Pool,Table}|{Pool,DB,Table}
  record :: term(),
  gen_idx :: integer() | undefined
}).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

i() ->
  F =
    fun(#record_cfg{key = K, db_name = DB, table_name = Table, share_func = Share, record = Rec, gen_idx = GID}, Acc) ->
      io:format("key:~p~n", [K]),
      io:format("\tdb:~ts~n", [DB]),
      io:format("\ttable:~ts~n", [Table]),
      io:format("\tshare:~p~n", [Share]),
      [RName | L] = tuple_to_list(Rec),
      io:format("\trecord_name:~p~n", [RName]),
      lists:foreach(
        fun({Name, Type}) -> io:format("\tcolumn:~ts, type:~p~n", [Name, Type]) end,
        L
      ),
      case is_integer(GID) of
        true ->
          {N, _} = lists:nth(GID - 1, L),
          io:format("\tgenerate_column:~ts~n", [N]);
        false ->
          pass
      end,
      Acc end,
  ets:foldl(F, [], ?EMYSQL_ORM_CFG_ETS),
  io:format("~n", []).

init([]) ->
  ets:new(?EMYSQL_ORM_CFG_ETS, [public, named_table, set, {keypos, #record_cfg.key}, {read_concurrency, true}]),
  add_pool(),
  {ok, #state{}}.

add_pool() ->
  {ok, Pools} = application:get_env(?MODULE, pools),
  F = fun({Pool, Cfg}) when is_map(Cfg) -> emysql:add_pool(Pool, maps:to_list(Cfg));
    ({Pool, Cfg}) when is_list(Cfg) -> emysql:add_pool(Pool, Cfg) end,
  lists:foreach(F, Pools).


check_record(Rec) ->
  [_Name | L] = erlang:tuple_to_list(Rec),
  F =
    fun({Name, Type}) when is_binary(Name), is_atom(Type) -> lists:member(Type, ?COL_TYPE_ALL);
      (_) -> false
    end,
  lists:all(F, L).

check_share_func({M, F}) when is_atom(M), is_atom(F) ->
  erlang:function_exported(M, F, 1);
check_share_func(Func) when is_function(Func, 1) ->
  true;
check_share_func(undefined) ->
  true;
check_share_func(_) ->
  false.

parse_record_cfg(Key, DBName, TableName, ShareFunc, Record) ->
  CheckRec = check_record(Record),
  CheckShareFunc = check_share_func(ShareFunc),
  if
    not CheckRec -> {fail, record_cfg_error};
    not CheckShareFunc -> {fail, share_func_error};
    true ->
      {ok, #record_cfg{key = Key, db_name = DBName, table_name = TableName, share_func = ShareFunc, record = Record}}
  end.

check_auto_gen(Gen, _Size) when Gen == undefined -> {ok, success};
check_auto_gen(Gen, Size) when is_integer(Gen) ->
  case Gen > 0 andalso Gen =< Size of
    true -> {ok, success};
    false -> {fail, auto_gen_error}
  end.

add_cfg(Key, DBName, TableName, ShareFunc, Record, GenIDX) ->
  case parse_record_cfg(Key, DBName, TableName, ShareFunc, Record) of
    {ok, CFG} ->
      Size = erlang:size(Record),
      case check_auto_gen(GenIDX, Size) of
        {ok, _} ->
          CFG2 = CFG#record_cfg{gen_idx = GenIDX},
          ets:insert(?EMYSQL_ORM_CFG_ETS, CFG2);
        Err ->
          Err
      end;
    Err ->
      Err
  end.

get_default_database()->
  {ok,Pools} = application:get_env(?MODULE,pools),
  case proplists:get_value(default,Pools,undefined) of
    #{database:= DB} -> {ok,DB};
    _ -> {error,not_found}
  end.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

gen_cond([], _RecList, Acc) -> lists:reverse(Acc);
gen_cond([{IDX, V} | Rest], RecList, Acc) when is_integer(IDX) ->
  Field = lists:nth(IDX, RecList),
  gen_cond(Rest, RecList, [{Field, ?COND_OP_EQ, V} | Acc]);
gen_cond([{IDX, OP, V} | Rest], RecList, Acc) when is_integer(IDX) ->
  Field = lists:nth(IDX, RecList),
  gen_cond(Rest, RecList, [{Field, OP, V} | Acc]).

gen_cond_by_example([RecName | L1], [RecName | L2]) ->
  gen_cond_by_example2(L1, L2, []).

gen_cond_by_example2([], [], Acc) -> lists:reverse(Acc);
gen_cond_by_example2([V | L1], [_ | L2], Acc) when V == undefined ->
  gen_cond_by_example2(L1, L2, Acc);
gen_cond_by_example2([V | L1], [E | L2], Acc) ->
  gen_cond_by_example2(L1, L2, [{E, ?COND_OP_EQ, V} | Acc]).

gen_order([], _RecList, Acc) -> lists:reverse(Acc);
gen_order([{IDX, V} | Rest], RecList, Acc) when is_integer(IDX) ->
  Field = lists:nth(IDX, RecList),
  gen_order(Rest, RecList, [{Field, V} | Acc]).

find(Key, Share) -> find(Key, Share, [], [], []).

find(Key, Share, Cond) -> find(Key, Share, Cond, [], []).

find(Key, Share, Cond, Order, Limit) ->
  [CFG] = ets:lookup(?EMYSQL_ORM_CFG_ETS, Key),
  #record_cfg{record = Rec} = CFG,
  Rec2List = erlang:tuple_to_list(Rec),
  [RecName | Fields] = Rec2List,
  {Pool, DB, Table} = find_pool_db_table(CFG, Share),
  Cond2 = case erlang:is_tuple(Cond) of
            true -> gen_cond_by_example(erlang:tuple_to_list(Cond), Rec2List);
            false -> gen_cond(Cond, Rec2List, [])
          end,
  Order2 = gen_order(Order, Rec2List, []),
  find_i(Pool, DB, Table, RecName, Fields, Cond2, Order2, Limit).

find_i(Pool, DB, Table, RecName, Fields, Cond, Order, Limit) ->

  WhereSQL = case Cond == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" WHERE ">>, make_cond_sql(Cond, <<" AND ">>, [])])
             end,

  OrderSQL = case Order == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" ORDER BY ">>, make_order_sql(Order, <<",">>, [])])
             end,

  SQL = erlang:iolist_to_binary([<<"SELECT ">>, make_cols_sql(Fields), <<" FROM ">>, make_db_table_sql(DB, Table), WhereSQL, OrderSQL, make_limit_sql(Limit)]),
  ?LOG_DEBUG("sql ~ts ~n", [SQL]),
  R = emysql:execute(Pool, SQL, ?DEF_TIMEOUT),
  % ?DEBUG("mysql server resp ~p ~n", [R]),
  case R of
    #result_packet{rows = L} when L == [] ->
      {ok, []};
    #result_packet{rows = L} ->
      {ok, parse_records(RecName, Fields, L, [])};
    #error_packet{msg = MSG} ->
      {fail, MSG}
  end.

count(Key, Share, Cond) ->
  [CFG] = ets:lookup(?EMYSQL_ORM_CFG_ETS, Key),
  #record_cfg{record = Record} = CFG,
  Record2List = erlang:tuple_to_list(Record),
  {Pool, DB, Table} = find_pool_db_table(CFG, Share),
  Cond2 = case erlang:is_tuple(Cond) of
            true -> gen_cond_by_example(erlang:tuple_to_list(Cond), Record2List);
            false -> gen_cond(Cond, Record2List, [])
          end,
  WhereSQL = case Cond2 == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" WHERE ">>, make_cond_sql(Cond2, <<" AND ">>, [])])
             end,
  SQL = erlang:iolist_to_binary([<<"SELECT COUNT(*) FROM ">>, make_db_table_sql(DB, Table), WhereSQL]),
  ?LOG_DEBUG("sql ~ts ~n", [SQL]),
  R = emysql:execute(Pool, SQL, ?DEF_TIMEOUT),
  % ?DEBUG("mysql server resp ~p ~n", [R]),
  case R of
    #result_packet{rows = [[Ret]]} ->
      {ok, Ret};
    #error_packet{msg = MSG} ->
      {fail, MSG}
  end.

calc_share(ShareFunc, Share) ->
  case ShareFunc of
    {M, F} -> M:F(Share);
    Func when is_function(Func, 1) -> Func(Share);
    undefined -> default
  end.

find_pool_db_table(#record_cfg{db_name = DBName, table_name = TableName, share_func = ShareFunc}, Share) ->
  case calc_share(ShareFunc, Share) of
    P when is_atom(P) -> {P, DBName, TableName};
    {P, DB0} -> {P, DB0, TableName};
    {P, DB0, Table0} -> {P, DB0, Table0}
  end.

make_cols_sql(L) ->
  L2 = lists:map(fun({Name, _}) -> Name end, L),
  erlang:iolist_to_binary(lists:join(<<",">>, L2)).

make_cond_sql([], Join, Acc) ->
  L = lists:reverse(Acc),
  erlang:iolist_to_binary(lists:join(Join, L));
make_cond_sql([{{ColName, ColType}, Op, V} | Rest], Join, Acc) ->
  E = erlang:iolist_to_binary([ColName, cond_op_2_bin(Op), field_v_2_bin(ColType, V)]),
  make_cond_sql(Rest, Join, [E | Acc]).

make_order_sql([], Join, Acc) ->
  L = lists:reverse(Acc),
  erlang:iolist_to_binary(lists:join(Join, L));
make_order_sql([{{ColName, _Type}, Order} | Rest], Join, Acc) ->
  Order2 = case Order of
             <<"ASC">> -> <<"ASC">>;
             <<"DESC">> -> <<"DESC">>;
             ?ORDER_TYPE_ASC -> <<"ASC">>;
             ?ORDER_TYPE_DESC -> <<"DESC">>
           end,
  E = erlang:iolist_to_binary([ColName, <<" ">>, Order2]),
  make_order_sql(Rest, Join, [E | Acc]).


cond_op_2_bin(?COND_OP_LT) -> <<" < ">>;
cond_op_2_bin(?COND_OP_LTE) -> <<" <= ">>;
cond_op_2_bin(?COND_OP_EQ) -> <<" = ">>;
cond_op_2_bin(?COND_OP_NEQ) -> <<" <> ">>;
cond_op_2_bin(?COND_OP_GTE) -> <<"  >= ">>;
cond_op_2_bin(?COND_OP_GT) -> <<" > ">>;
cond_op_2_bin(?COND_OP_LIKE) -> <<" LIKE ">>.

date_to_string({Y, M, D}) ->
  lists:flatten(io_lib:format("~B-~2.10.0B-~2.10.0B", [Y, M, D]));
date_to_string(O) -> O.

time_to_string({Y, M, D}) ->
  time_to_string({{Y, M, D}, {0, 0, 0}});
time_to_string({{Y, M, D}, {H, MM, S}}) ->
  lists:flatten(io_lib:format("~B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B", [Y, M, D, H, MM, S])).

bin_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).

field_v_2_bin(Type, Value) when Type == ?COL_TYPE_BOOL ->
  case Value of
    true -> <<"1">>;
    false -> <<"0">>
  end;
field_v_2_bin(Type, Value) when Type == ?COL_TYPE_INTEGER ->
  erlang:integer_to_binary(Value);
field_v_2_bin(Type, Value) when Type == ?COL_TYPE_STRING ->
  Bin = case erlang:is_binary(Value) of
          true -> Value;
          false -> unicode:characters_to_binary(Value)
        end,
  field_v_2_bin(?COL_TYPE_BINARY, Bin);
field_v_2_bin(Type, Value) when Type == ?COL_TYPE_DATETIME ->
  erlang:iolist_to_binary(["'", time_to_string(Value), "'"]);
field_v_2_bin(Type, Value) when Type == ?COL_TYPE_DATE ->
  erlang:iolist_to_binary(["'", date_to_string(Value), "'"]);
field_v_2_bin(Type, Value) when Type == ?COL_TYPE_TERM ->
  Bin = erlang:term_to_binary(Value),
  field_v_2_bin(?COL_TYPE_BINARY, Bin);
field_v_2_bin(Type, Value) when Type == ?COL_TYPE_BINARY ->
  L = bin_to_hexstr(Value),
  erlang:iolist_to_binary(["x'", erlang:list_to_binary(L), "'"]).

make_limit_sql([]) -> <<"">>;
make_limit_sql([E]) when is_integer(E) ->
  erlang:iolist_to_binary([<<" LIMIT ">>, erlang:integer_to_binary(E)]);
make_limit_sql([A, B]) when is_integer(A), is_integer(B) ->
  erlang:iolist_to_binary([<<" LIMIT ">>, erlang:integer_to_binary(A), <<",">>, erlang:integer_to_binary(B)]).

make_db_table_sql(DB, Table) ->
  case is_binary(DB) of
    true -> erlang:iolist_to_binary([DB, <<".">>, Table]);
    false -> Table
  end.

parse_fs([], [], Acc) -> lists:reverse(Acc);
parse_fs([V | L], [{_Name, Type} | R], Acc) when Type == ?COL_TYPE_BOOL ->
  V2 = is_integer(V) andalso V =/= 0,
  parse_fs(L, R, [V2 | Acc]);
parse_fs([V | L], [{_Name, Type} | R], Acc) when Type == ?COL_TYPE_DATETIME ->
  DateTime = case V of
               {datetime, V2} -> V2;
               _ -> undefined
             end,
  parse_fs(L, R, [DateTime | Acc]);
parse_fs([V | L], [{_Name, Type} | R], Acc) when Type == ?COL_TYPE_DATE ->
  Date = case V of
           {date, V2} -> V2;
           _ -> undefined
         end,
  parse_fs(L, R, [Date | Acc]);
parse_fs([V | L], [{_Name, Type} | R], Acc) when Type == ?COL_TYPE_TERM ->
  V2 = case erlang:is_binary(V) of
         true -> erlang:binary_to_term(V);
         false -> undefined
       end,
  parse_fs(L, R, [V2 | Acc]);
parse_fs([V | L], [_ | R], Acc) ->
  parse_fs(L, R, [V | Acc]).

parse_records(_RecName, _FS, [], Acc) -> lists:reverse(Acc);
parse_records(RecName, FS, [L | Rest], Acc) ->
  L1 = parse_fs(L, FS, []),
  L2 = [RecName | L1],
  Rec = erlang:list_to_tuple(L2),
  parse_records(RecName, FS, Rest, [Rec | Acc]).

make_fields_sql([], [], Acc1, Acc2) ->
  F = fun(L) ->
    L2 = lists:reverse(L),
    L3 = lists:join(",", L2),
    erlang:iolist_to_binary([<<"(">>] ++ L3 ++ [<<")">>])
      end,
  {F(Acc1), F(Acc2)};
make_fields_sql([undefined | L1], [_ | L2], Acc1, Acc2) ->
  % undefined field
  make_fields_sql(L1, L2, Acc1, Acc2);
make_fields_sql([V | L1], [{ColName, ColType} | L2], Acc1, Acc2) ->
  make_fields_sql(L1, L2, [ColName | Acc1], [field_v_2_bin(ColType, V) | Acc2]).

insert(Key, Share, Rec) -> insert_replace(insert, Key, Share, Rec).

replace(Key, Share, Rec) -> insert_replace(replace, Key, Share, Rec).

insert_replace(Type, Key, Share, Rec) ->
  [CFG] = ets:lookup(?EMYSQL_ORM_CFG_ETS, Key),
  #record_cfg{record = Record, gen_idx = GenIDX} = CFG,
  Record2List = erlang:tuple_to_list(Record),
  [RecName | Fields] = Record2List,
  {Pool, DB, Table} = find_pool_db_table(CFG, Share),
  L = erlang:tuple_to_list(Rec),
  [RecName | L2] = L,
  {ColSQL, ValuesSQL} = make_fields_sql(L2, Fields, [], []),
  SQL = case Type of
          insert ->
            erlang:iolist_to_binary([<<"INSERT INTO ">>, make_db_table_sql(DB, Table), ColSQL, <<" VALUES ">>, ValuesSQL]);
          replace ->
            erlang:iolist_to_binary([<<"REPLACE INTO ">>, make_db_table_sql(DB, Table), ColSQL, <<" VALUES ">>, ValuesSQL])
        end,
  ?LOG_DEBUG("sql ~s ~n", [SQL]),
  R = emysql:execute(Pool, SQL, ?DEF_TIMEOUT),
  %?DEBUG("mysql server resp ~p ~n", [R]),
  case R of
    #ok_packet{insert_id = GenID} when is_integer(GenIDX) ->
      L3 = lists:zip(lists:seq(1, erlang:size(Rec)), L),
      L4 = case lists:keyfind(GenIDX, 1, L3) of
             {_, _} -> lists:keyreplace(GenIDX, 1, L3, {GenIDX, GenID});
             false -> L3
           end,
      L5 = lists:map(fun({_, X}) -> X end, L4),
      {ok, erlang:list_to_tuple(L5)};
    #ok_packet{} ->
      {ok, Rec};
    #error_packet{msg = MSG} ->
      {fail, MSG}
  end.

update(Key, Share, Update, Cond) ->
  update(Key, Share, Update, Cond, [], []).

update(Key, Share, Update, Cond, Order, Limit) ->
  [CFG] = ets:lookup(?EMYSQL_ORM_CFG_ETS, Key),
  #record_cfg{record = Record} = CFG,
  Record2List = erlang:tuple_to_list(Record),
  {Pool, DB, Table} = find_pool_db_table(CFG, Share),
  Cond2 = case erlang:is_tuple(Cond) of
            true -> gen_cond_by_example(erlang:tuple_to_list(Cond), Record2List);
            false -> gen_cond(Cond, Record2List, [])
          end,
  Update2 = case erlang:is_tuple(Update) of
              true -> gen_cond_by_example(erlang:tuple_to_list(Update), Record2List);
              false -> gen_cond(Update, Record2List, [])
            end,

  Order2 = gen_order(Order, Record2List, []),
  [RecName | Fields] = Record2List,
  update_i(Pool, DB, Table, RecName, Fields, Cond2, Update2, Order2, Limit).

update_i(_Pool, _DB, _Table, _RecName, _Fields, _Cond, Update, _, _) when Update == [] -> {ok, pass};
update_i(Pool, DB, Table, _RecName, _Fields, Cond, Update, Order, Limit) ->
  WhereSQL = case Cond == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" WHERE ">>, make_cond_sql(Cond, <<" AND ">>, [])])
             end,

  UpdateSQL = erlang:iolist_to_binary([<<" SET ">>, make_cond_sql(Update, <<" , ">>, [])]),

  OrderSQL = case Order == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" ORDER BY ">>, make_order_sql(Order, <<",">>, [])])
             end,

  SQL = erlang:iolist_to_binary([<<"UPDATE ">>, make_db_table_sql(DB, Table), UpdateSQL, WhereSQL, OrderSQL, make_limit_sql(Limit)]),
  ?LOG_DEBUG("sql ~ts ~n", [SQL]),
  R = emysql:execute(Pool, SQL, ?DEF_TIMEOUT),
  % ?DEBUG("mysql server resp ~p ~n", [R]),
  case R of
    #ok_packet{affected_rows = Aff, warning_count = Warn} -> {ok, {Aff, Warn}};
    #error_packet{msg = MSG} -> {fail, MSG}
  end.

delete(Key, Share, Cond) -> delete(Key, Share, Cond, [], []).

delete(Key, Share, Cond, Order, Limit) ->
  [CFG] = ets:lookup(?EMYSQL_ORM_CFG_ETS, Key),
  #record_cfg{record = Record} = CFG,
  Record2List = erlang:tuple_to_list(Record),
  {Pool, DB, Table} = find_pool_db_table(CFG, Share),
  Cond2 = case erlang:is_tuple(Cond) of
            true -> gen_cond_by_example(erlang:tuple_to_list(Cond), Record2List);
            false -> gen_cond(Cond, Record2List, [])
          end,
  Order2 = gen_order(Order, Record2List, []),
  [RecName | Fields] = Record2List,
  delete_i(Pool, DB, Table, RecName, Fields, Cond2, Order2, Limit).

delete_i(Pool, DB, Table, _RecName, _Fields, Cond, Order, Limit) ->
  WhereSQL = case Cond == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" WHERE ">>, make_cond_sql(Cond, <<" AND ">>, [])])
             end,

  OrderSQL = case Order == [] of
               true -> <<"">>;
               false -> erlang:iolist_to_binary([<<" ORDER BY ">>, make_order_sql(Order, <<",">>, [])])
             end,

  SQL = erlang:iolist_to_binary([<<"DELETE FROM ">>, make_db_table_sql(DB, Table), WhereSQL, OrderSQL, make_limit_sql(Limit)]),
  ?LOG_DEBUG("sql ~ts ~n", [SQL]),
  R = emysql:execute(Pool, SQL, ?DEF_TIMEOUT),
  % ?DEBUG("mysql server resp ~p ~n", [R]),
  case R of
    #ok_packet{affected_rows = Aff, warning_count = Warn} -> {ok, {Aff, Warn}};
    #error_packet{msg = MSG} -> {fail, MSG}
  end.