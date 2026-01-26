-module(embe_qdrant_rest_priv).

-export([
        create_collection/2
      , create_collection/3
      , status/1
      , status/2
      , create_point/2
      , create_point/3
      , get_point/2
      , get_point/3
      , update_payload/2
      , update_payload/3
      , points_query/2
      , points_query/3
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
    Db1 = klsn_binstr:urlencode(Db0),
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
    Db1 = klsn_binstr:urlencode(Db0),
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
    Db1 = klsn_binstr:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary, "/points?wait=true">>,
    Req = {Url, [], "application/json", jsone:encode(Data)},
    Res = httpc:request(put, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, _}} when 200=<Stat,Stat=<299 ->
            ok
    end.

% GET /collections/{collection_name}/points/{id}
-spec get_point(db(), unicode:unicode_binary() | integer()) -> payload().
get_point(Db, Id) ->
    get_point(Db, Id, #{with_payload=>true, with_vector=>true}).

-spec get_point(
        db(),
        unicode:unicode_binary() | integer(),
        #{with_payload := boolean(), with_vector := boolean()}
    ) -> payload().
get_point(Db, Id, Opt) ->
    get_point(Db, Id, Opt, db_info()).

-spec get_point(
        db(),
        unicode:unicode_binary() | integer(),
        #{with_payload := boolean(), with_vector := boolean()},
        info()
    ) -> payload().
get_point(Db, Id, Opt, Info) when is_atom(Db) ->
    get_point(atom_to_binary(Db), Id, Opt, Info);
get_point(Db0, Id0, #{with_payload:=WithPayload, with_vector:=WithVector}, #{url:=Url0}) ->
    Db1 = klsn_binstr:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Id = case Id0 of
        N when is_integer(N) -> integer_to_binary(N);
        B when is_binary(B) -> B
    end,
    Id1 = klsn_binstr:urlencode(Id),
    Q = iolist_to_binary([
        <<"?with_payload=">>, bool_bin(WithPayload),
        <<"&with_vector=">>, bool_bin(WithVector)
    ]),
    Url = <<Url0/binary, Db/binary, "/points/", Id1/binary, Q/binary>>,
    Req = {Url, []},
    Res = httpc:request(get, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Data}} when 200=<Stat,Stat=<299 ->
            jsone:decode(Data);
        {ok, {{_, 404, _}, _, _}} ->
            error(not_found)
    end.


-spec update_payload(db(), payload()) -> ok.
update_payload(Db, Data0) ->
    update_payload(Db, Data0, db_info()).

-spec update_payload(db(), payload(), info()) -> ok.
update_payload(Db, Payload, Info) when is_atom(Db) ->
    update_payload(atom_to_binary(Db), Payload, Info);
update_payload(Db0, Data, #{url:=Url0}) ->
    Db1 = klsn_binstr:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary, "/points/payload?wait=true">>,
    Req = {Url, [], "application/json", jsone:encode(Data)},
    Res = httpc:request(put, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, _}} when 200=<Stat,Stat=<299 ->
            ok
    end.

-spec points_query(db(), payload()) -> ok.
points_query(Db, Data0) ->
    points_query(Db, Data0, db_info()).

-spec points_query(db(), payload(), info()) -> ok.
points_query(Db, Payload, Info) when is_atom(Db) ->
    points_query(atom_to_binary(Db), Payload, Info);
points_query(Db0, Data, #{url:=Url0}) ->
    Db1 = klsn_binstr:urlencode(Db0),
    Db = <<"/collections/", Db1/binary>>,
    Url = <<Url0/binary, Db/binary, "/points/query">>,
    Req = {Url, [], "application/json", jsone:encode(Data)},
    Res = httpc:request(post, Req, [], [{body_format, binary}]),
    case Res of
        {ok, {{_, Stat, _}, _, Body}} when 200=<Stat,Stat=<299 ->
            jsone:decode(Body)
    end.

-spec search(db(), payload()) -> ok.
search(Db, Data0) ->
    search(Db, Data0, db_info()).

-spec search(db(), payload(), info()) -> ok.
search(Db, Payload, Info) when is_atom(Db) ->
    search(atom_to_binary(Db), Payload, Info);
search(Db0, Data, #{url:=Url0}) ->
    Db1 = klsn_binstr:urlencode(Db0),
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

bool_bin(true) -> <<"true">>;
bool_bin(false) -> <<"false">>.
