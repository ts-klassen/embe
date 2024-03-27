# embe

embeddings for erlang

## Build

    $ rebar3 compile

## Add to rebar3 dependency

```
{deps, [
    {embe, {git, "https://github.com/ts-klassen/embe.git", {tag, "1.0.0"}}}
]}.
```

## Setup

You will need OpenAI API access, CouchDB and Qdrant.

### OpenAI API

Set `OPENAI_API_KEY` env with your api key.
You may skip this if you are only using the `embe_vector_db` module.

    $ export OPENAI_API_KEY="sk-****"

### CouchDB

Set `COUCHDB_URL` env with your CouchDB api url.
Or use the default url of `http://localhost:5984`.

    $ export COUCHDB_URL="https://user:pass@couchdb.example.com"

### Qdrant

Set `QDRANT_URL` env with your Qdrant api url.
Or use the default url of `http://localhost:6333`.

    $ export QDRANT_URL="https://user:pass@qdrant.example.com"

### initial setup function

To use the `embe` module, run `embe:init_setup/0` to setup the database.

```
1> embe:init_setup().
ok
```

If you are only using the `embe_vector_db` module, use `embe_vector_db:init/0` instead.

## Usage

### embe

```
1> Test = embe:new().
#{collection => <<"embe-3072-cosine-text-embedding-3-large">>,
  distance => cosine,
  embeddings_function => #Fun<embe.0.41628754>,
  model => <<"text-embedding-3-large">>,
  name => <<"_embe_default_namespace">>,size => 3072}
2> embe:add(<<"This is a test.">>, Test).
ok
3> embe:add(<<"This is an example.">>, Test).
ok
4> embe:search(<<"For experimental purposes">>, 2, Test). 
[<<"This is a test.">>,<<"This is an example.">>]
```

### embe_vector_db

```
1> embe_vector_db:create_collection(another_test, 3, cosine).
ok
2> embe_vector_db:upsert_point(another_test, [1,2,3], fun(none) ->
2>     #{count => 1}   
2> end).
#{count => 1,<<"C">> => <<"2024-03-26T21:36:17.696+09:00">>,
  <<"U">> => <<"2024-03-26T21:36:17.696+09:00">>,
  <<"_id">> =>
      <<"another_test:e515da7d686fcae57d60be8936538ae14061d53b3ffc66388da76391a6424ac5">>,
  <<"_rev">> => <<"1-649f0f712ca18429649975fe7dc1a6c0">>,
  <<"id">> => 0}
3> embe_vector_db:upsert_point(another_test, [1,2,3], fun({value, Doc}) ->    
3>     Count = maps:get(<<"count">>, Doc),
3>     Doc#{<<"count">> => Count+1}
3> end).
#{<<"C">> => <<"2024-03-26T21:36:17.696+09:00">>,
  <<"U">> => <<"2024-03-26T21:40:37.759+09:00">>,
  <<"_id">> =>
      <<"another_test:e515da7d686fcae57d60be8936538ae14061d53b3ffc66388da76391a6424ac5">>,
  <<"_rev">> => <<"2-200d0682817bc7a002d4582ffa1c8ae5">>,
  <<"count">> => 2,<<"id">> => 0}
4> embe_vector_db:search(another_test, [1,2,3]).
[#{<<"C">> => <<"2024-03-26T21:36:17.696+09:00">>,
   <<"U">> => <<"2024-03-26T21:40:37.759+09:00">>,
   <<"_id">> =>
       <<"another_test:e515da7d686fcae57d60be8936538ae14061d53b3ffc66388da76391a6424ac5">>,
   <<"_rev">> => <<"2-200d0682817bc7a002d4582ffa1c8ae5">>,
   <<"count">> => 2,<<"id">> => 0}]
```
