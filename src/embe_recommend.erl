-module(embe_recommend).

-export([
        init/0
      , get/1
      , lookup/1
      , positive/2
      , neutral/2
      , negative/2
    ]).
-export_type([
        collection_name/0
      , namespace/0
      , id/0
      , key/0
    ]).

-type collection_name() :: atom() | unicode:unicode_binary().
-type namespace() :: atom() | unicode:unicode_binary().
-type id() :: unicode:unicode_binary().
-type key() :: {collection_name(), namespace(), id()}.
-type recommend() :: #{
        recommend := #{
            positive => [embe:id()]
          , negative => [embe:id()]
        } 
    }.

-spec init() -> ok.
init() ->
    try klsn_db:create_db(?MODULE) of
        _ -> ok
    catch
        error:exists -> ok
    end.


-spec get(key()) -> recommend().
get(Key) ->
    maybe_to_recommend(lookup(Key)).


-spec lookup(key()) -> klsn:maybe(recommend()).
lookup(Key) ->
    case klsn_db:lookup(?MODULE, key_to_couch_id(Key)) of
        none ->
            none;
        {value, Doc} ->
            maybe_to_recommend({value, Doc})
    end.

-spec positive(key(), embe:id()) -> ok. 
positive(Key, Id) ->
    klsn_db:upsert(?MODULE, key_to_couch_id(Key), fun(MaybeDoc)->
        Doc = maybe_to_recommend(MaybeDoc),
        add_id(positive, Id, delete_id(Id, Doc))
    end),
    ok.


-spec neutral(key(), embe:id()) -> ok.
neutral(Key, Id) ->
    klsn_db:upsert(?MODULE, key_to_couch_id(Key), fun(MaybeDoc)->
        Doc = maybe_to_recommend(MaybeDoc),
        delete_id(Id, Doc)
    end),
    ok.


-spec negative(key(), embe:id()) -> ok. 
negative(Key, Id) ->
    klsn_db:upsert(?MODULE, key_to_couch_id(Key), fun(MaybeDoc)->
        Doc = maybe_to_recommend(MaybeDoc),
        add_id(negative, Id, delete_id(Id, Doc))
    end),
    ok.


-spec add_id(positive | negative, embe:id(), recommend()) -> recommend().
add_id(Type, Id, Doc) ->
    update_recommend([recommend, Type], fun(List)->
        [Id|List]
    end, Doc).


-spec delete_id(embe:id(), recommend()) -> recommend().
delete_id(Id, Doc0) ->
    Doc10 = update_recommend([recommend, positive], fun(List10)->
        lists:delete(Id, List10)
    end, Doc0),
    update_recommend([recommend, negative], fun(List20)->
        lists:delete(Id, List20)
    end, Doc10).


-spec update_recommend(
        klsn_map:path(), fun(([embe:id()])->[embe:id()]), recommend()
    ) -> recommend().
update_recommend(Path, Fun, Doc) ->
    klsn_map:upsert(Path, Fun(klsn_map:get(Path, Doc)), Doc).


-spec key_to_couch_id(key()) -> klsn_db:id().
key_to_couch_id({Collection, Name, Id}) ->
    ToBinary = fun(KeyElem) ->
        klsn_binstr:from_any(KeyElem)
    end,
    CollectionBin = ToBinary(Collection),
    NameBin = ToBinary(Name),
    IdBin = ToBinary(Id),
    <<CollectionBin/binary, ":", NameBin/binary, ":", IdBin/binary>>.


-spec maybe_to_recommend(klsn:maybe(recommend() | #{})) -> recommend().
maybe_to_recommend(Maybe) ->
    case Maybe of
        {value, Rec=#{recommend:=_}} ->
            Rec;
        {value, Doc} ->
            #{
                recommend => #{
                    positive => klsn_map:get([<<"recommend">>, <<"positive">>], Doc, [])
                  , negative => klsn_map:get([<<"recommend">>, <<"negative">>], Doc, [])
                }
            };
        none ->
            #{
                recommend => #{
                    positive => []
                  , negative => []
                }
            }
    end.
