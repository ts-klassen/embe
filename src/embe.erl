-module(embe).

-export([
        init_setup/0
      , new/0
      , new/1
      , add/2
      , get/2
      , lookup/2
      , update/3
      , update/4
      , search/2
      , search/3
      , recommendation_key/1
      , recommendation_key/2
      , positive/2
      , neutral/2
      , negative/2
      , recommendations/1
      , recommendations/2
    ]).

-export_type([
        embeddings/0
      , namespace/0
      , metadata/0
      , update_function/0
      , upsert_function/0
      , id/0
      , key/0
      , search_option/0
      , search_result/0
      , recommendation_key/0
    ]).

-type embeddings() :: #{
        model := unicode:unicode_binary()
      , size := non_neg_integer()
      , distance := embe_vector_db:distance()
      , name := namespace()
      , collection := atom() | unicode:unicode_binary()
      , embeddings_function := fun((unicode:unicode_binary())->[float()])
      , recommendation_key := recommendation_key()
    }.

-type namespace() :: atom() | unicode:unicode_binary().

-type metadata() :: #{
        input := unicode:unicode_binary()
      , key() => key()
    }.

-type update_function() :: fun((metadata())->metadata()).

-type upsert_function() :: fun((klsn:maybe(metadata()))->metadata()).

-type id() :: unicode:unicode_binary().

-type key() :: atom() | unicode:unicode_binary().

-type search_option() :: #{
        limit => pos_integer()
      , filter => [{key(), key() | [key()]}]
      , id_only => boolean() % default false
    }.

-type search_result() :: [id() | #{

    }].

-type recommendation_key() :: atom() | unicode:unicode_binary().

-spec init_setup() -> ok.
init_setup() ->
    embe_vector_db:init(),
    embe_recommend:init(),
    % For default text-embedding-3-large model.
    create_db(new()),
    ok.

-spec new() -> embeddings().
new() ->
    new(<<"embe_default_namespace">>).

-spec new(namespace()) -> embeddings().
new(Name) ->
    Model = <<"text-embedding-3-large">>,
    #{
        model => Model
      , size => 3072
      , distance => cosine
      , name => Name
      , collection => <<"embe-3072-cosine-", Model/binary>>
      , embeddings_function => fun(Input) ->
            gpte_embeddings:simple(Input, Model)
        end
      , recommendation_key => <<"embe_default_recommendation_key">>
    }.

-spec create_db(embeddings()) -> ok.
create_db(#{size:=Size, collection:=Collection, distance:=Distance}) ->
    try embe_vector_db:create_collection(Collection, Size, Distance) of
        ok -> ok
    catch
        error:conflict -> ok
    end.

-spec add(
        metadata()
      , embeddings()
   ) -> id().
add(MetaData, Opts=#{name:=Name, collection:=Collection, model:=Model, embeddings_function:=T2V}) ->
    Input = case MetaData of
        #{ input := Input0 } -> Input0;
        #{ <<"input">> := Input0 } -> Input0;
        _ -> erlang:error(badarg, [MetaData, Opts])
    end,
    Vector = T2V(Input),
    NameSpace = to_binary(Name),
    embe_vector_db:insert(Collection, #{
        namespace => NameSpace
      , metadata => MetaData
      , model => Model
      , version => 1
    }, Vector).

-spec get(
        id(), embeddings()
   ) -> metadata().
get(Id, Embe) ->
    case lookup(Id, Embe) of
        {value, Metadata} ->
            Metadata;
        none ->
            erlang:error(not_found, [Id, Embe])
    end.

-spec lookup(
        id(), embeddings()
   ) -> klsn:maybe(metadata()).
lookup(Id, #{name:=NameSpace, collection:=Collection}) ->
    case embe_vector_db:lookup(Collection, Id) of
        {value, #{<<"metadata">>:=Metadata, <<"namespace">>:=NS}} when NS =:= NameSpace ->
            {value, Metadata};
        _ ->
            none
    end.

-spec update(
        id(), update_function(), embeddings()
   ) -> metadata().
update(Id, Fun, #{name:=Name, collection:=Collection}) ->
    NameSpace = to_binary(Name),
    Payload = embe_vector_db:update(Collection, Id, fun
        (#{<<"metadata">>:=Metadata, <<"namespace">>:=NS}=Doc) when NS =:= NameSpace ->
            Doc#{<<"metadata">>:=Fun(Metadata)};
        (_) ->
            erlang:error(not_found)
    end),
    maps:get(<<"metadata">>, Payload).

-spec update(
        id(), update_function(), unicode:unicode_binary(), embeddings()
   ) -> metadata().
update(Id, Input, Fun, #{name:=Name, collection:=Collection, embeddings_function:=T2V}) ->
    NameSpace = to_binary(Name),
    Payload = embe_vector_db:update(Collection, Id, fun
        (#{<<"metadata">>:=Metadata, <<"namespace">>:=NS}=Doc) when NS =:= NameSpace ->
            Doc#{<<"metadata">>:=Fun(Metadata), <<"input">>:=Input};
        (_) ->
            erlang:error(not_found)
    end, T2V(Input)),
    maps:get(<<"metadata">>, Payload).

-spec search(
        id(), embeddings()
    ) -> search_result().
search(Input, Opt) ->
    search(Input, #{}, Opt).

-spec search(
        id(), search_option(), embeddings()
    ) -> search_result().
search(Id, SearchOption, #{name:=Name, collection:=Collection}) ->
    Filter = case SearchOption of
        #{filter := Fltr} ->
            lists:map(fun
                ({Key, Any}) when is_list(Any) ->
                    #{
                        key => nest([metadata, Key])
                      , match => #{ any => Any }
                    };
                ({Key, Value}) ->
                    #{
                        key => nest([metadata, Key])
                      , match => #{ value => Value }
                    }
            end, Fltr);
        _ ->
            []
    end,
    Result =  embe_vector_db:search(Collection, Id, #{
        q => #{
            limit => maps:get(limit, SearchOption, 10)
          , filter => #{
                must => [
                    #{key => <<"namespace">>, match => #{value => Name}}
                | Filter]
            }
        }
      , score => true
      , vector_id => true
    }),
    lists:map(fun(ResElem=#{<<"_vector_id">>:=VectorId})->
       case SearchOption of
           #{id_only := true} ->
               VectorId;
           _ ->
               ResElem#{<<"id">> => VectorId} 
       end
    end, Result).

-spec recommendation_query(
        embeddings()
    ) -> embe_recommend:recommend() | embe_vector_db:vector().
recommendation_query(#{collection:=K1, name:=K2, recommendation_key:=K3, size:=S}) ->
    case embe_recommend:get({K1, K2, K3}) of
        #{recommend:=#{positive:=[],negative:=[]}} ->
            lists:duplicate(S, 0.0);
        Recommend ->
            Recommend
    end.

-spec recommendations(embeddings()) -> search_result().
recommendations(Embe) ->
    search(recommendation_query(Embe), Embe).

-spec recommendations(search_option(), embeddings()) -> search_result().
recommendations(Opts, Embe) ->
    search(recommendation_query(Embe), Opts, Embe).

-spec recommendation_key(embeddings()) -> recommendation_key().
recommendation_key(Embe) ->
    klsn_map:get([recommendation_key], Embe).

-spec recommendation_key(recommendation_key(), embeddings()) -> embeddings().
recommendation_key(Key, Embe) ->
    klsn_map:upsert([recommendation_key], Key, Embe).

-spec positive(id(), embeddings()) -> ok.
positive(Id, #{collection:=K1, name:=K2, recommendation_key:=K3}) ->
    embe_recommend:positive({K1, K2, K3}, Id).

-spec neutral(id(), embeddings()) -> ok.
neutral(Id, #{collection:=K1, name:=K2, recommendation_key:=K3}) ->
    embe_recommend:neutral({K1, K2, K3}, Id).

-spec negative(id(), embeddings()) -> ok.
negative(Id, #{collection:=K1, name:=K2, recommendation_key:=K3}) ->
    embe_recommend:negative({K1, K2, K3}, Id).


to_binary(Name) ->
    case Name of
        NS when is_atom(NS) ->
            atom_to_binary(NS);
        NS when is_binary(NS) ->
            NS
    end.

-spec nest([key()]) -> unicode:unicode_binary().
nest(Path0) ->
    Path10 = lists:map(fun to_binary/1, Path0),
    iolist_to_binary(lists:join(<<".">>, Path10)).

