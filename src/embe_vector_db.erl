-module(embe_vector_db).

-export([
        init/0
      , create_collection/3
      , upsert_point/3
      , search/2
      , search/3
      , vector_hash/1
      , collection_exist/1
    ]).
-export_type([
        collection_name/0
      , distance/0
      , vector/0
      , payload/0
      , search_option/0
      , upsert_function/0
    ]).

% https://qdrant.github.io/qdrant/redoc/index.html

-type collection_name() :: atom().
-type distance() :: dot | cosine.
-type vector() :: [float(), ...].
-type payload() :: map().
-type search_option() :: #{
        q := Query :: #{}
      , exact => false | true | remove
      , score => false | true
    }.
-type upsert_function() :: fun((klsn:maybe(payload()))->payload()).

-spec init() -> ok.
init() ->
    try embe_couchdb_priv:create_db(?MODULE) of
        _ -> ok
    catch
        error:exists -> ok
    end.

-spec create_collection(
        collection_name(), non_neg_integer(), distance()
    ) -> ok.
create_collection(Name, Size, Dist) when is_atom(Name) ->
    create_collection(atom_to_binary(Name), Size, Dist);
create_collection(Name, Size, Dist) ->
    embe_couchdb_priv:create_doc(?MODULE, #{
        '_id' => Name
      , version => 1
      , type => collection
      , latest_id => -1
      , size => Size
      , distance => Dist
      , exists => false
    }),
    DistName = case Dist of
        dot -> <<"Dot">>;
        cosine -> <<"Cosine">>
    end,
    embe_qdrant_rest_priv:create_collection(Name, #{
        vectors => #{
            size => Size
          , distance => DistName
        }
    }),
    embe_couchdb_priv:update(?MODULE, Name, fun(Doc) ->
        Doc#{<<"exists">>:=true}
    end),
    ok.


-spec upsert_point(
        collection_name(), vector(), upsert_function()
    ) -> payload().
upsert_point(Name, Vector, Fun) when is_atom(Name) ->
    upsert_point(atom_to_binary(Name), Vector, Fun);
upsert_point(Name, Vector, Fun) ->
    Hash = vector_hash(Vector),
    CouchId = <<Name/binary, ":", Hash/binary>>,
    Payload = embe_couchdb_priv:upsert(?MODULE, CouchId, fun
        (none) ->
            Doc = Fun(none),
            #{  <<"latest_id">>:=QdrantId
            } = embe_couchdb_priv:update(?MODULE, Name, fun(Meta) ->
                #{<<"latest_id">>:=LatestId} = Meta,
                Meta#{<<"latest_id">>:=LatestId+1}
            end),
            Doc#{<<"id">> => QdrantId};
        ({value, Doc}) ->
            Fun({value, Doc})
    end),
    embe_qdrant_rest_priv:create_point(Name, #{points=>[#{
        id => maps:get(<<"id">>, Payload)
      , vector => Vector
      , payload => Payload
    }]}),
    Payload.

-spec search(collection_name(), vector()) -> [payload()].
search(Name, Vect) ->
    search(Name, Vect, #{q=>#{top=>10}}).

-spec search(collection_name(), vector(), search_option()) -> [payload()].
search(Name, Vect, Opt) when is_atom(Name) ->
    search(atom_to_binary(Name), Vect, Opt);
search(Name, Vect, Opt=#{q:=Query}) ->
    #{<<"result">>:=Res} = embe_qdrant_rest_priv:search(Name, Query#{
        vector => Vect
      , with_payload => true
    }),
    lists:filtermap(fun(#{<<"payload">>:=Payload0, <<"score">>:=Score}) ->
        Payload = case Opt of
            #{score:=true} ->
                Payload0#{<<"_score">>=>Score};
            _ ->
                Payload0
        end,
        case {Opt, Score} of
            {#{exact:=true}, N} when N < 0.999999999999 ->
                false;
            {#{exact:=remove}, N} when N > 0.999999999999 ->
                false;
            _ ->
                {true, Payload}
        end
    end, Res).

-spec collection_exist(collection_name()) -> boolean().
collection_exist(Name) when is_atom(Name) ->
    collection_exist(atom_to_binary(Name));
collection_exist(Name) ->
    case embe_couchdb_priv:lookup(?MODULE, Name) of
        none -> false;
        _ -> true
    end.

vector_hash(List) when is_list(List) ->
    list_to_binary(string:lowercase(binary_to_list(binary:encode_hex(crypto:hash(sha256, term_to_binary(List)))))).

