-module(embe).

-export([
        init_setup/0
      , new/0
      , new/1
      , add/2
      , search/2
      , search/3
      , hash/1
    ]).

-export_type([
        embeddings/0
    ]).

-type embeddings() :: #{
        model := unicode:unicode_binary()
      , size := non_neg_integer()
      , distance := embe_vector_db:distance()
      , name := atom() | unicode:unicode_binary()
      , collection := atom() | unicode:unicode_binary()
      , embeddings_function := fun((unicode:unicode_binary())->[float()])
    }.

-spec init_setup() -> ok.
init_setup() ->
    embe_vector_db:init(),
    try embe_couchdb_priv:create_db(?MODULE) of
        _ -> ok
    catch
        error:exists -> ok
    end,
    create_db(new(<<>>)),
    ok.

-spec new() -> embeddings().
new() ->
    new(<<"_embe_default_namespace">>).

-spec new(atom() | unicode:unicode_binary()) -> embeddings().
new(Name) ->
    Model = <<"text-embedding-3-large">>,
    #{
        model => Model
      , size => 3072
      , distance => cosine
      , name => Name
      , collection => <<"embe-3072-cosine-", Model/binary>>
      , embeddings_function => fun(Input) ->
            embed(Model, Input)
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
        unicode:unicode_binary(), embeddings()
   ) -> ok | {error, exists}.
add(Input, #{name:=Name, collection:=Collection, embeddings_function:=T2V}) ->
    Vector = T2V(Input),
    Payload = #{
        namespace => #{
            Name => [Input]
        }
    },
    NameSpace = to_binary(Name),
    try embe_vector_db:upsert_point(Collection, Vector, fun
        ({value, #{<<"namespace">>:=#{NameSpace:=[In|_]}}}) when In =:= Input ->
            erlang:throw({?MODULE, exists});
        ({value, Doc=#{<<"namespace">>:=#{NameSpace:=Inputs}}}) ->
            klsn_map:upsert([<<"namespace">>, NameSpace], [Input|Inputs], Doc);
        ({value, Doc}) ->
            klsn_map:upsert([<<"namespace">>, NameSpace], [Input], Doc);
        (none) ->
            Payload
    end) of
        _ ->
            ok
    catch
        throw:{?MODULE, exists} ->
            {error, exists}
    end.

-spec search(
        unicode:unicode_binary(), embeddings()
    ) -> [unicode:unicode_binary()].
search(Input, Opt) ->
    search(Input, 1, Opt).

-spec search(
        unicode:unicode_binary(), non_neg_integer(), embeddings()
    ) -> [unicode:unicode_binary()].
search(Input, Top, #{name:=Name, collection:=Collection, embeddings_function:=T2V}) ->
    Vector = T2V(Input),
    NameSpace = to_binary(Name),
    Opt = #{q=>#{
        top => Top
      , filter => #{must => [#{
            key => <<"namespace.", NameSpace/binary>>
          , values_count => #{
                gt => 0
            }
        }]}
    }},
    List = embe_vector_db:search(Collection, Vector, Opt),
    Res = lists:map(fun(#{<<"namespace">>:=#{NameSpace:=Text}})->
        Text
    end, List),
    lists:append(Res).

embed(Model, Input) ->
    Hash = hash(Input),
    Key = <<Model/binary, ":", Hash/binary>>,
    case embe_couchdb_priv:lookup_attachment(?MODULE, Key, <<"vector">>) of
        {value, Vector} ->
            binary_to_term(zlib:uncompress(Vector));
        none ->
            Vector = gpte_embeddings:simple(Input, Model),
            spawn(fun()->
                Data = zlib:compress(term_to_binary(Vector)),
                embe_couchdb_priv:upload_attachment(?MODULE, Key, <<"vector">>, Data),
                embe_couchdb_priv:update(?MODULE, Key, fun(Doc) -> Doc#{
                    version => 1
                  , model => Model
                  , input => Input
                } end)
            end),
            Vector
    end.

to_binary(Name) ->
    case Name of
        NS when is_atom(NS) ->
            atom_to_binary(NS);
        NS when is_binary(NS) ->
            NS
    end.

hash(Term) ->
    list_to_binary(string:lowercase(binary_to_list(binary:encode_hex(crypto:hash(sha256, term_to_binary(Term)))))).
