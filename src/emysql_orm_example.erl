-module(emysql_orm_example).

%% API
-export([
  create_table/0,
  add_cfg/0,
  add/2,
  find/2
]).

-record(example, {id, age, name}).

-define(DB_NAME, <<"test">>).
-define(TABLE_NAME, <<"t_example">>).
-define(DEF_TIMEOUT, 5000).
-define(POOL, default).
-define(DEFAULT_KEY, ?MODULE).
-define(DEFAULT_SHARE, 0).

add_cfg() ->
  Rec = #example{
    id = {<<"id">>, integer},
    age = {<<"age">>, integer},
    name = {<<"name">>, string}
  },
  emysql_orm:add_cfg(?DEFAULT_KEY, ?DB_NAME, ?TABLE_NAME, undefined, Rec, #example.id).

create_table() ->
  SQL = <<"CREATE TABLE t_example (id bigint(20) auto_increment, age int(10), name varchar(256), primary key (id) )">>,
  emysql:execute(?POOL, SQL, ?DEF_TIMEOUT).


add(Age, Name) ->
  Example = #example{age = Age, name = Name},
  emysql_orm:insert(?DEFAULT_KEY, ?DEFAULT_SHARE, Example).

find(Age, Name) ->
  Example = #example{age = Age, name = Name},
  emysql_orm:find(?DEFAULT_KEY, ?DEFAULT_SHARE, Example).






