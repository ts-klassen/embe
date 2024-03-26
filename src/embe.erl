-module(embe).

-export([
        init_setup/0
      , new/1
      , create_db/1
      , add/2
      , search/3
    ]).

-export_type([
        embeddings/0
    ]).

-type embeddings() :: #{
        model := unicode:unicode_binary()
      , size := non_neg_integer()
      , distance := embe_vector_db:distance()
      , name := atom() | unicode:unicode_binary()
    }.

-spec init_setup() -> ok.
init_setup() ->
    embe_vector_db:init().

-spec new(atom() | unicode:unicode_binary()) -> embeddings().
new(Name) ->
    #{
        model => <<"text-embedding-3-large">>
      , size => 3072
      , distance => cosine
      , name => Name
    }.

-spec create_db(embeddings()) -> ok.
create_db(#{size:=Size, name:=Name, distance:=Distance}) ->
    embe_vector_db:create_collection(Name, Size, Distance).

-spec add(unicode:unicode_binary(), embeddings()) -> embe_vector_db:vector().
add(Input, #{name:=Name, model:=Model}) ->
    Vector = gpte_embeddings:simple(Input, Model),
    Payload = #{
        input => Input
    },
    embe_vector_db:upsert_point(Name, Vector, fun
        (none) ->
            Payload;
        ({value, _}) ->
            erlang:error(exists)
    end).

-spec search(
        unicode:unicode_binary(), non_neg_integer(), embeddings()
    ) -> [unicode:unicode_binary(), ...].
search(Input, Top, #{name:=Name, model:=Model}) ->
    Vector = gpte_embeddings:simple(Input, Model),
    Opt = #{q=>#{
        top => Top
    }},
    List = embe_vector_db:search(Name, Vector, Opt),
    lists:map(fun(#{<<"input">>:=Text})->
        Text
    end, List).
