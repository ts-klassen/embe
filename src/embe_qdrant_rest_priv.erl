-module(embe_qdrant_rest_priv).

-export([
        create_collection/2
      , create_collection/3
      , status/1
      , status/2
      , create_point/2
      , create_point/3
      , search/2
      , search/3
    ]).
-export_type([
        info/0
      , db/0
      , payload/0
      , collection_payload/0
    ]).

% https://qdrant.github.io/qdrant/redoc/index.html

-type info() :: #{
        url := unicode:unicode_binary()
    }.
-type db() :: unicode:unicode_binary().
-type payload() :: maps:map(atom() | unicode:unicode_binary(), value()).
-type value() :: atom()
               | unicode:unicode_binary()
               | lists:list(value())
               | maps:map(atom() | unicode:unicode_binary(), value())
               .

-type collection_payload() :: #{
        vectors => #{
                size => non_neg_integer()
              , distance => 'Dot' | 'Cosine'
            }
    }.

-spec create_collection(db(), collection_payload()) -> ok.
create_collection(Db, CollectionPayload) ->
    create_collection(Db, CollectionPayload, db_info()).

-spec create_collection(db(), collection_payload(), info()) -> ok.
create_collection(Db, CollectionPayload, Info) when is_atom(Db) ->
    create_collection(atom_to_binary(Db), CollectionPayload, Info);
create_collection(Db0, CollectionPayload, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary>>,
    Req = {Url, [], "application/json", jsone:encode(CollectionPayload)},
    Res = httpc:request(put, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, _}} when 200=<Stat,Stat=<299 ->
            ok;
        {ok, {{_, 409, _}, _, _}} ->
            error(exists)
    end.

-spec status(db()) -> payload().
status(Db) ->
    status(Db, db_info()).

-spec status(db(), info()) -> payload().
status(Db, Info) when is_atom(Db) ->
    status(atom_to_binary(Db), Info);
status(Db0, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary>>,
    Req = {Url, []},
    Res = httpc:request(get, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Data}} when 200=<Stat,Stat=<299 ->
            jsone:decode(Data);
        {ok, {{_, 404, _}, _, _}} ->
            error(not_found)
    end.


-spec create_point(db(), payload()) -> ok.
create_point(Db, Data0) ->
    create_point(Db, Data0, db_info()).

-spec create_point(db(), payload(), info()) -> ok.
create_point(Db, Payload, Info) when is_atom(Db) ->
    create_point(atom_to_binary(Db), Payload, Info);
create_point(Db0, Data, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary, "/points?wait=true">>,
    Req = {Url, [], "application/json", jsone:encode(Data)},
    Res = httpc:request(put, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, _}} when 200=<Stat,Stat=<299 ->
            ok
    end.

-spec search(db(), payload()) -> ok.
search(Db, Data0) ->
    search(Db, Data0, db_info()).

-spec search(db(), payload(), info()) -> ok.
search(Db, Payload, Info) when is_atom(Db) ->
    search(atom_to_binary(Db), Payload, Info);
search(Db0, Data, #{url:=Url0}) ->
    Db1 = cow_qs:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary, "/points/search">>,
    Req = {Url, [], "application/json", jsone:encode(Data)},
    Res = httpc:request(post, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Body}} when 200=<Stat,Stat=<299 ->
            jsone:decode(Body)
    end.

db_info() ->
    Url = case os:getenv("QDRANT_URL") of
        false ->
            <<"http://localhost:6333">>;
        Str when is_list(Str) ->
            list_to_binary(Str)
    end,
    #{url=>Url}.

