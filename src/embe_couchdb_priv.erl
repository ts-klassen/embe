-module(embe_couchdb_priv).

-export([
        create_db/1
      , create_db/2
      , create_doc/2
      , create_doc/3
      , get/2
      , get/3
      , lookup/2
      , lookup/3
      , update/3
      , update/4
      , upsert/3
      , upsert/4
      , lookup_attachment/3
      , lookup_attachment/4
      , upload_attachment/4
      , upload_attachment/5
      , time_now/0
      , new_id/0
    ]).
-export_type([
        info/0
      , db/0
      , key/0
      , payload/0
      , value/0
      , id/0
      , rev/0
      , update_function/0
      , upsert_function/0
    ]).

-type info() :: #{
        url := unicode:unicode_binary()
    }.
-type db() :: unicode:unicode_binary().
-type key() :: unicode:unicode_binary().
-type id() :: unicode:unicode_binary().
-type rev() :: unicode:unicode_binary().
-type payload() :: maps:map(atom() | unicode:unicode_binary(), value()).
-type value() :: atom()
               | unicode:unicode_binary()
               | lists:list(value())
               | maps:map(atom() | unicode:unicode_binary(), value())
               .
-type update_function() :: fun((payload())->payload()).
-type upsert_function() :: fun((klsn:maybe(payload()))->payload()).

-spec create_db(db()) -> ok.
create_db(Db) ->
    create_db(Db, db_info()).

-spec create_db(db(), info()) -> ok.
create_db(Db, Info) when is_atom(Db) ->
    create_db(atom_to_binary(Db), Info);
create_db(Db0, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary>>,
    Res = httpc:request(put, {Url, []}, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, _}} when 200=<Stat,Stat=<299 ->
            ok;
        {ok, {{_, 412, _}, _, _}} ->
            error(exists)
    end.


-spec create_doc(db(), payload()) -> {id(), rev()}.
create_doc(Db, Data0) ->
    create_doc(Db, Data0, db_info()).

-spec create_doc(db(), payload(), info()) -> {id(), rev()}.
create_doc(Db, Data0, Info) ->
    Data2 = remove_keys(['_rev', 'C', 'U'], Data0),
    TimeNow = time_now(),
    Data = Data2#{<<"U">>=>TimeNow, <<"C">>=>TimeNow},
    post(Db, Data, Info).

-spec get(db(), key()) -> payload().
get(Db, Key) ->
    get(Db, Key, db_info()).

-spec get(db(), key(), info()) -> payload().
get(Db, Key, Info) ->
    case lookup(Db, Key, Info) of
        {value, Value} -> Value;
        none -> error(not_found)
    end.

-spec lookup(db(), key()) -> klsn:maybe(payload()).
lookup(Db, Key) ->
    lookup(Db, Key, db_info()).

-spec lookup(db(), key(), info()) -> klsn:maybe(payload()).
lookup(Db, Key, Info) when is_atom(Db) ->
    lookup(atom_to_binary(Db), Key, Info);
lookup(Db0, Key0, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/", Db1/binary>>,
    Key1 = cow_qs:urlencode(Key0),
    Key = <<"/", Key1/binary>>,
    Url = <<Url0/binary, Db/binary, Key/binary>>,
    Res = httpc:request(get, {Url, []}, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Data}} when 200=<Stat,Stat=<299->
            {value, jsone:decode(Data)};
        {ok, {{_, 404, _}, _, _}} ->
            none
    end.

-spec post(db(), payload()) -> {id(), rev()}.
post(Db, Payload) ->
    post(Db, Payload, db_info()).

-spec post(db(), payload(), info()) -> {id(), rev()}.
post(Db, Payload, Info) when is_atom(Db) ->
    post(atom_to_binary(Db), Payload, Info);
post(Db0, Payload0, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/", Db1/binary>>,
    Payload = jsone:encode(Payload0),
    Url = <<Url0/binary, Db/binary>>,
    Res = httpc:request(post, {Url, [], "application/json", Payload}, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Data}} when 200=<Stat,Stat=<299 ->
            #{<<"ok">>:=true,<<"id">>:=Id,<<"rev">>:=Rev} = jsone:decode(Data),
            {Id, Rev};
        {ok, {{_, 404, _}, _, _}} ->
            error(not_found);
        {ok, {{_, 409, _}, _, _}} ->
            error(conflict)
    end.

-spec update(db(), key(), update_function()) -> payload().
update(Db, Key, Fun) ->
    update(Db, Key, Fun, db_info()).

-spec update(db(), key(), update_function(), info()) -> payload().
update(Db, Key, Fun0, Info) ->
    Fun = fun
        (none) ->
            error(not_found);
        ({value, Data}) ->
            Fun0(Data)
    end,
    upsert_(Db, Key, Fun, Info, 10).

-spec upsert(db(), key(), upsert_function()) -> payload().
upsert(Db, Key, Fun) ->
    upsert(Db, Key, Fun, db_info()).

-spec upsert(db(), key(), upsert_function(), info()
    ) -> payload().
upsert(Db, Key, Fun, Info) ->
    upsert_(Db, Key, Fun, Info, 10).

upsert_(_Db, _Key, _Fun, _Info, ReTry) when ReTry =< 0 ->
    error(too_many_retry);
upsert_(Db, Key, Fun, Info, Retry) ->
    MaybeData = lookup(Db, Key, Info),
    Data0 = Fun(MaybeData),
    Data1 = remove_keys(['_id', 'C', 'U'], Data0),
    Data2 = Data1#{<<"_id">>=>Key},
    TimeNow = time_now(),
    Data3 = Data2#{<<"U">>=>TimeNow},
    Data = case MaybeData of
        {value, #{<<"C">>:=C}} -> Data3#{<<"C">>=>C};
        _ -> Data3#{<<"C">>=>TimeNow}
    end,
    try
        post(Db, Data)
    of
        {Id, Rev} ->
            Data#{
                <<"_id">> => Id
              , <<"_rev">> => Rev
            }
    catch
        error:conflict ->
            timer:sleep(100),
            upsert_(Db, Key, Fun, Info, Retry-1);
        throw:Error ->
            throw(Error);
        Class:Error:Stack ->
            spawn(fun()-> erlang:raise(Class,Error,Stack) end),
            timer:sleep(500),
            upsert_(Db, Key, Fun, Info, Retry-5)
    end.


-spec lookup_attachment(
        db(), key(), unicode:unicode_binary()
    ) -> klsn:maybe(binary()).
lookup_attachment(Db, Key, Name) ->
    lookup_attachment(Db, Key, Name, db_info()).

-spec lookup_attachment(
        db(), key(), info(), unicode:unicode_binary()
    ) -> klsn:maybe(binary()).
lookup_attachment(Db, Key, Name, Info) when is_atom(Db) ->
    lookup_attachment(atom_to_binary(Db), Key, Name, Info);
lookup_attachment(Db0, Key0, Name0, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/", Db1/binary>>,
    Key1 = cow_qs:urlencode(Key0),
    Key = <<"/", Key1/binary>>,
    Name1 = cow_qs:urlencode(Name0),
    Name = <<"/", Name1/binary>>,
    Url = <<Url0/binary, Db/binary, Key/binary, Name/binary>>,
    Res = httpc:request(get, {Url, []}, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Data}} when 200=<Stat,Stat=<299->
            {value, Data};
        {ok, {{_, 404, _}, _, _}} ->
            none
    end.


-spec upload_attachment(
        db(), key(), binary(), unicode:unicode_binary()
    ) -> {id(), rev()}.
upload_attachment(Db, Key, Name, Data) ->
    upload_attachment(Db, Key, Name, Data, db_info()).

-spec upload_attachment(
        db(), key(), info(), binary(), unicode:unicode_binary()
    ) -> {id(), rev()}.
upload_attachment(Db, Key, Name, Data, Info) when is_atom(Db) ->
    upload_attachment(atom_to_binary(Db), Key, Name, Data, Info);
upload_attachment(Db0, Key0, Name0, Data, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/", Db1/binary>>,
    Key1 = cow_qs:urlencode(Key0),
    Key = <<"/", Key1/binary>>,
    Name1 = cow_qs:urlencode(Name0),
    Name = <<"/", Name1/binary>>,
    Url = <<Url0/binary, Db/binary, Key/binary, Name/binary>>,
    Res = httpc:request(put, {Url, [], "application/octet-stream", Data}, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Body}} when 200=<Stat,Stat=<299 ->
            #{<<"ok">>:=true,<<"id">>:=Id,<<"rev">>:=Rev} = jsone:decode(Body),
            {Id, Rev};
        {ok, {{_, 404, _}, _, _}} ->
            error(not_found);
        {ok, {{_, 409, _}, _, _}} ->
            error(conflict)
    end.



-spec time_now() -> unicode:unicode_binary().
time_now() ->
    list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(millisecond), [{unit, millisecond}, {offset, "+09:00"}])).

-spec remove_keys([atom()], map()) -> map().
remove_keys(Keys, Map) when is_list(Keys), is_map(Map) ->
    lists:foldl(fun(Key, Data0) ->
        Data1 = maps:remove(Key, Data0),
        maps:remove(atom_to_binary(Key), Data1)
    end, Map, Keys).


new_id() ->
    Ref = make_ref(),
    Time = erlang:system_time(second),
    Str0 = ref_to_list(Ref),
    [_|Str1] = lists:reverse(Str0),
    Str2 = lists:reverse(Str1),
    [_,A1,A2,A3] = string:split(Str2, ".", all),
    N1 = list_to_integer(A1),
    N2 = list_to_integer(A2),
    N3 = list_to_integer(A3),
    List = lists:flatten([
        string:casefold(integer_to_list(Time, 36)),
        "-",
        string:casefold(integer_to_list(N1, 36)),
        "-",
        string:casefold(integer_to_list(N2, 36)),
        "-",
        string:casefold(integer_to_list(N3, 36))
    ]),
    list_to_binary(List).

db_info() ->
    Url = case os:getenv("COUCHDB_URL") of
        false ->
            <<"http://localhost:5984">>;
        Str when is_list(Str) ->
            list_to_binary(Str)
    end,
    #{url=>Url}.

