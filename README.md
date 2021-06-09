# emysql_orm
> erlang emysql orm tools


### example
```
1> emysql_orm_example:start_link().
{ok,<0.166.0>}
2> rd(example,{id, age, name, term}).
example
3> E = #example{age = 10,name = <<"name">>,term = {123}}.
#example{id = undefined,age = 10,name = <<"name">>,
         term = {123}}
4> emysql_orm:insert(emysql_orm_example,default,E).
{ok,#example{id = 1,age = 10,name = <<"name">>,term = {123}}}
5> emysql_orm_example:find_term_by_id(1).
{ok,#example{id = undefined,age = undefined,name = undefined, term = {123}}}
6> emysql_orm_example:find_name_term_by_age(10).
{ok,[#example{id = undefined,age = undefined,name = <<"name">>,term = {123}}]}
7> E2 = #example{age = 10,name = <<"name2">>,term = {true}}. 
#example{id = undefined,age = 10,name = <<"name2">>,term = {true}}
8> emysql_orm:insert(emysql_orm_example,default,E2).
{ok,#example{id = 2,age = 10,name = <<"name2">>,term = {true}}}
9> emysql_orm_example:find_name_term_by_age(10).
{ok,[#example{id = undefined,age = undefined,name = <<"name">>, term = {123}},
     #example{id = undefined,age = undefined,name = <<"name2">>,term = {true}}]}
```