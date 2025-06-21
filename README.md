# embe

embeddings for erlang

## Build

    $ rebar3 compile

## Add to rebar3 dependency

```
{deps, [
    {embe, {git, "https://github.com/ts-klassen/embe.git", {tag, "2.0.0"}}}
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
  embeddings_function => #Fun<embe.0.65375742>,
  model => <<"text-embedding-3-large">>,
  name => <<"embe_default_namespace">>,
  recommendation_key => <<"embe_default_recommendation_key">>,
  size => 3072}
2> embe:add(#{input => <<"This is a test.">>}, Test).
<<"cbea8ed7-aa16-418c-a01b-89c6aad876cd">>
3> embe:add(#{input => <<"This is an example.">>}, Test).
<<"9b6c04ee-ea52-42b1-82df-874d61040bd1">>
4> embe:search(<<"cbea8ed7-aa16-418c-a01b-89c6aad876cd">>, #{id_only => true}, Test). 
[<<"9b6c04ee-ea52-42b1-82df-874d61040bd1">>]
```

### embe_vector_db

```
1> embe_vector_db:create_collection(another_test, 3, cosine).
ok
2> Id = embe_vector_db:insert(another_test, #{count => 1}, [1,2,3]).
<<"20a33107-2d68-449e-b7ae-a51f52bd50e8">>
3> embe_vector_db:update(another_test, Id, fun(Doc=#{<<"count">>:=Count}) ->
3>     Doc#{<<"count">> => Count+1}
3> end).
#{<<"C">> => <<"2025-06-21T20:47:01.331+09:00">>,
  <<"U">> => <<"2025-06-21T20:50:02.892+09:00">>,
  <<"_id">> =>
      <<"another_test:20a33107-2d68-449e-b7ae-a51f52bd50e8">>, 
  <<"_rev">> => <<"2-cb4acb0b9b0c108500310f0554ea122c">>,
  <<"count">> => 2}
4> embe_vector_db:search(another_test, [1,2,3]).
[#{<<"C">> => <<"2025-06-21T20:47:01.331+09:00">>,
   <<"U">> => <<"2025-06-21T20:50:02.892+09:00">>,
   <<"_id">> =>
       <<"another_test:20a33107-2d68-449e-b7ae-a51f52bd50e8">>,
   <<"_rev">> => <<"2-cb4acb0b9b0c108500310f0554ea122c">>,
   <<"count">> => 2}]
```
