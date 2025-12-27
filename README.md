# Introduction
Flask-multi-redis is a batteries-included, simple-to-use Flask extension provide support for Redis. It can be 
configured for a single database or multiple databases. Redis is an in-memory data store used by millions of 
developers as a cache, vector database, document database, streaming engine, and message broker. Redis has 
built-in replication and different levels of on-disk persistence. It supports complex data types (for example, 
strings, hashes, lists, sets, sorted sets, and JSON), with atomic operations defined on those data types.

This package is released under GPL2 only.

# Examples
The following examples are just simple examples, for more detailed examples see the tests folder.

## Single database connection
In the flask startup logic for FlaskRedisSingle class; 

```python
    app = Flask( ... )
    ...
    FlaskRedisSingle( app, strict = False )
    ...
```

The configuration for FlaskRedisSingle class in the Flask configuration  
```yaml
REDIS:
  DATABASE:   1
  NAME:       ExampleDatabase
  HOST:       localhost.localzone
  PASSWORD:   a-secure-password
  PORT:       6379
```

## Single database connection

In the flask startup logic for FlaskRedisMulti class; 

```python
    app = Flask( ... )
    ...
    FlaskRedisMulti( app, strict = False )

```

The configuration for FlaskRedisSingle class 
```yaml
REDIS:
  HOST:       localhost.localzone
  PASSWORD:   a-secure-password
  PORT:       6379
  DATABASES:  
    General:            0
    ExampleDatabase:    1
    OtherDatabase:      2
```

## Accessing the Redis database
Getting and setting a property on the database.

```python
    redis = current_app.extensions['redis']
    session = redis.getRedis( 'ExampleDatabase' )
    value = session.get( 'propery' )

    session.set( 'propery', value )

```

## Message broker
Posting a message on the broker and wait for a reply

```python
    redis = current_app.extensions['redis']
    session = redis.getRedis( 'ExampleDatabase' )
    pubsub = session.subscribe( 'queue-name' )
    session.publish( 'queue-name', "Hello world" )
    
    reply = session.wait_for_reply( pubsub, str, timeout = 5, encoding = 'utf-8' )

```

# Documentation

# Installation
To install Flask-Db-Admin using pip, simply:

```bash
    pip install flask-multi-redis
```
    
# Contributing
If you are a developer working on and maintaining Flask-Db-Admin, checkout the repo by doing:

```bash
    git clone https://github.com/pe2mbs/flask-extended-config.git
    cd flask-multi-redis
```

# Tests
Tests are run with pytest. If you are not familiar with this package, you can find out more 
on their website.
