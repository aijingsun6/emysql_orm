-module(emysql_orm_example).
-include_lib("kernel/include/logger.hrl").
-behaviour(gen_server).

-export([
  start_link/0,
  insert_n/1,
  find_by_id/1,
  find_by_age/1,
  find_term_by_id/1,
  find_name_term_by_age/1
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).
-define(DB_NAME, <<"test">>).
-define(TABLE_NAME, <<"t_example">>).
-define(DEF_TIMEOUT, 5000).
-define(POOL, default).
-define(DEFAULT_KEY, ?MODULE).
-define(DEFAULT_SHARE, 0).
-record(state, {}).
-record(example, {id, age, name, term}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  add_cfg(),
  create_table(),
  {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
add_cfg() ->
  Rec = #example{
    id = {<<"id">>, integer},
    age = {<<"age">>, integer},
    name = {<<"name">>, string},
    term = {<<"term">>, term, fun(X) -> erlang:term_to_binary(X) end, fun(X) -> erlang:binary_to_term(X) end}
  },
  emysql_orm:add_cfg(?DEFAULT_KEY, ?DB_NAME, ?TABLE_NAME, undefined, Rec, #example.id).

create_table() ->
  SQL = <<"DROP TABLE IF EXISTS `t_example`;
   CREATE TABLE `t_example` (
      `id` bigint(20) auto_increment,
      `age` int(10),
      `name` varchar(256),
      `term` blob,
      primary key (id)
   )ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;">>,
  emysql:execute(?POOL, SQL, ?DEF_TIMEOUT).

%% insert N example
insert_n(N) ->
  L = lists:seq(1, N),
  F = fun(X) ->
    Name = base64:encode(crypto:strong_rand_bytes(32)),
    Term = {X, Name},
    E = #example{age = X, name = Name, term = Term},
    emysql_orm:insert(?DEFAULT_KEY, ?DEFAULT_SHARE, E) end,
  lists:foreach(F, L).

find_by_id(ID) when is_integer(ID) ->
  Example = #example{id = ID},
  case emysql_orm:find(?DEFAULT_KEY, ?DEFAULT_SHARE, Example) of
    {ok, [R | _]} -> {ok, R};
    {ok, []} -> {error, not_found}
  end.

find_term_by_id(ID) when is_integer(ID) ->
  Example = #example{id = ID},
  case emysql_orm:find_fields(?DEFAULT_KEY, ?DEFAULT_SHARE, [#example.term], Example) of
    {ok, [R | _]} -> {ok, R};
    {ok, []} -> {error, not_found}
  end.


find_by_age(Age) when is_integer(Age) ->
  Example = #example{age = Age},
  emysql_orm:find(?DEFAULT_KEY, ?DEFAULT_SHARE, Example).

find_name_term_by_age(Age) when is_integer(Age) ->
  Example = #example{age = Age},
  emysql_orm:find_fields(?DEFAULT_KEY, ?DEFAULT_SHARE, [#example.name, #example.term], Example).


