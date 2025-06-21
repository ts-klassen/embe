-module(embe).

-export([
        init_setup/0
      , new/0
      , new/1
      , add/2
      , update/3
      , update/4
      , search/2
      , search/3
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
    ]).

-type embeddings() :: #{
        model := unicode:unicode_binary()
      , size := non_neg_integer()
      , distance := embe_vector_db:distance()
      , name := namespace()
      , collection := atom() | unicode:unicode_binary()
      , embeddings_function := fun((unicode:unicode_binary())->[float()])
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
    }.

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
    ) -> [unicode:unicode_binary()].
search(Input, Opt) ->
    search(Input, #{}, Opt).

-spec search(
        id(), search_option(), embeddings()
    ) -> [unicode:unicode_binary()].
search(Id, SearchOption0, #{name:=Name, collection:=Collection}) ->
    SearchOption10 = maps:merge(#{
        limit => 10
    }, SearchOption0),
    Filter = case SearchOption10 of
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
    embe_vector_db:search(Collection, Id, #{
        q => #{
            limit => maps:get(limit, SearchOption10)
          , filter => #{
                must => [
                    #{key => <<"namespace">>, match => #{value => Name}}
                | Filter]
            }
        }
      , score => true
    }).

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

