-module(embe_vector_db).

-export([
        init/0
      , create_collection/3
      , insert/2
      , insert/3
      , insert/4
      , get/2
      , lookup/2
      , update/3
      , update/4
      , upsert/4
      , search/2
      , search/3
      , vector_hash/1
      , collection_exist/1
      , uuid/0
    ]).
-export_type([
        collection_name/0
      , id/0
      , distance/0
      , vector/0
      , payload/0
      , search_option/0
      , update_function/0
      , upsert_function/0
    ]).

% https://qdrant.github.io/qdrant/redoc/index.html

-type collection_name() :: atom().
-type id() :: unicode:unicode_binary().
-type distance() :: dot | cosine.
-type vector() :: [float(), ...].
-type payload() :: map().
-type search_option() :: #{
        q := Query :: #{}
      , exact => false | true | remove
      , score => false | true
      , vector_id => false | true
    }.
-type update_function() :: fun((payload())->payload()).
-type upsert_function() :: fun((klsn:maybe(payload()))->payload()).

-spec init() -> ok.
init() ->
    try klsn_db:create_db(?MODULE) of
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
    klsn_db:create_doc(?MODULE, #{
        '_id' => Name
      , version => 1
      , type => collection
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
    klsn_db:update(?MODULE, Name, fun(Doc) ->
        Doc#{<<"exists">>:=true}
    end),
    ok.


-spec insert(
        collection_name(), vector()
    ) -> id().
insert(Name, Vector) ->
    Id = uuid(),
    insert(Name, Id, #{}, Vector),
    Id.


-spec insert(
        collection_name(), payload(), vector()
    ) -> id().
insert(Name, Payload, Vector) ->
    Id = uuid(),
    insert(Name, Id, Payload, Vector),
    Id.


-spec insert(
        collection_name(), id(), payload(), vector()
    ) -> payload().
insert(Name, Id, Payload, Vector) ->
    upsert(Name, Id, fun
        (none) -> Payload;
        (_) -> erlang:error(exists)
    end, {value, Vector}).


-spec get(
        collection_name(), id()
    ) -> payload().
get(Name, Id) ->
    case lookup(Name, Id) of
        {value, Payload} ->
            Payload;
        none ->
            erlang:error(not_found, [Name, Id])
    end.


-spec lookup(
        collection_name(), id()
    ) -> klsn:maybe(payload()).
lookup(Name, Id) ->
    CouchId = <<Name/binary, ":", Id/binary>>,
    klsn_db:lookup(?MODULE, CouchId).


-spec update(
        collection_name(), id(), update_function()
    ) -> payload().
update(Name, Id, Fun) ->
    upsert(Name, Id, fun
        ({value, Doc}) -> Fun(Doc);
        (_) -> erlang:error(not_found)
    end, none).


-spec update(
        collection_name(), id(), update_function(), vector()
    ) -> payload().
update(Name, Id, Fun, Vector) ->
    upsert(Name, Id, fun
        ({value, Doc}) -> Fun(Doc);
        (_) -> erlang:error(not_found)
    end, {value, Vector}).

-spec upsert(
        collection_name(), id(), upsert_function(), klsn:maybe(vector())
    ) -> payload().
upsert(Name, Id, Fun, MaybeVector) when is_atom(Name) ->
    upsert(atom_to_binary(Name), Id, Fun, MaybeVector);
upsert(Name, Id, Fun, MaybeVector) when is_integer(Id) ->
    upsert(Name, integer_to_binary(Id), Fun, MaybeVector);
upsert(Name, Id, Fun, MaybeVector) ->
    CouchId = <<Name/binary, ":", Id/binary>>,
    Payload = klsn_db:upsert(?MODULE, CouchId, fun
        (none) ->
            Doc = Fun(none),
            case MaybeVector of
                {value, _} ->
                    ok;
                _ ->
                    erlang:error(vector_required)
            end,
            Doc;
        ({value, Doc}) ->
            Fun({value, Doc})
    end),
    case MaybeVector of
        {value, Vector} ->
            embe_qdrant_rest_priv:create_point(Name, #{points=>[#{
                id => Id
              , vector => Vector
              , payload => Payload
            }]});
        _ ->
            embe_qdrant_rest_priv:update_payload(Name, #{
                points => [Id]
              , payload => Payload
            })
    end,
    Payload.

-spec search(collection_name(), vector()) -> [payload()].
search(Name, Vect) ->
    search(Name, Vect, #{q=>#{top=>10}}).

-spec search(collection_name(), id() | vector(), search_option()) -> [payload()].
search(Name, Vect, Opt) when is_atom(Name) ->
    search(atom_to_binary(Name), Vect, Opt);
search(Name, Vect, Opt=#{q:=Query}) ->
    #{<<"result">>:=Res} = embe_qdrant_rest_priv:points_query(Name, Query#{
        'query' => Vect
      , with_payload => true
    }),
    lists:filtermap(fun
        (#{<<"id">>:=VectorId,<<"payload">>:=Payload0, <<"score">>:=Score}) ->
            Payload10 = case Opt of
                #{score:=true} ->
                    Payload0#{<<"_score">>=>Score};
                _ ->
                    Payload0
            end,
            Payload = case Opt of
                #{vector_id:=true} ->
                    Payload10#{<<"_vector_id">>=>VectorId};
                _ ->
                    Payload10
            end,
            case {Opt, Score} of
                {#{exact:=true}, N} when N < 0.999999999999 ->
                    false;
                {#{exact:=remove}, N} when N > 0.999999999999 ->
                    false;
                _ ->
                    {true, Payload}
        end
    end, maps:get(<<"points">>, Res)).

-spec collection_exist(collection_name()) -> boolean().
collection_exist(Name) when is_atom(Name) ->
    collection_exist(atom_to_binary(Name));
collection_exist(Name) ->
    case klsn_db:lookup(?MODULE, Name) of
        none -> false;
        _ -> true
    end.


-spec uuid() -> id().
uuid() ->
    <<R1:48, _:4, R2:12, _:2, R3:62>> = crypto:strong_rand_bytes(16),
    <<U1:32, U2:16, U3:16, U4:16, U5:48>> = <<R1:48, 0:1, 1:1, 0:1, 0:1, R2:12, 1:1, 0:1, R3:62>>,
    iolist_to_binary(io_lib:format(
        "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
        [U1, U2, U3, U4, U5])
    ).

vector_hash(List) when is_list(List) ->
    list_to_binary(string:lowercase(binary_to_list(binary:encode_hex(crypto:hash(sha256, term_to_binary(List)))))).

