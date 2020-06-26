-module(emysql_orm_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
  application:ensure_started(crypto),
  application:ensure_started(emysql),
  emysql_orm_sup:start_link().

stop(_State) ->
  ok.

