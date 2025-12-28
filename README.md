# Introduction
Flask-multi-redis is a batteries-included, simple-to-use Flask extension provide support for Redis. It can be 
configured for a single database or multiple databases. Redis is an in-memory data store used by millions of 
developers as a cache, vector database, document database, streaming engine, and message broker. Redis has 
built-in replication and different levels of on-disk persistence. It supports complex data types (for example, 
strings, hashes, lists, sets, sorted sets, and JSON), with atomic operations defined on those data types.

This package is released under GPL2 only.

This package is created to support multiple databases in the flask web application, that support multiple backend 
processes. This to separate the data attributes per backend process. 

# Examples
The following examples are just simple examples, for more detailed examples see the Documentation section.

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

## Multi database connection
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

NOTE: The broker functions with the database, this means that publish and subscribes are independent from 
      the selected database. 

# Documentation

## FlaskRedisSingle class
### FlaskRedisSingle.__init__( self, app: t.Optional[ t.Union[ 'Flask', dict, IRedisSimpleConfig ] ] = None, strict: t.Optional[ bool ] = True, config_element: t.Optional[ str ] = "REDIS", prefix: t.Optional[ str ] = None,  **kwargs )       
Create a single redis database connection.

parameter 'app' can be a Flask application instance, or dictionary containing the configuration, or IRedisSimpleConfig pydantic class with the configuration.
parameter 'strict' is a boolean flag determines the redis.Redis or redis.StrictRedis class to be used for the connection, default is True, therefor redis.StrictRedis is used.
parameter 'config_element' is used when the Flask configuration is used to find the section, default is "REDIS".
parameter 'prefix' is an optional name that is prefix to the publish and subscription names to avoid conflicts between different environments running though the same redis server.   
'kwargs' are optional keyword arguments that are passed to the redis.Redis or redis.StrictRedis class.

#### Example instantiate with Flask application:
```yaml
REDIS:
    SCHEMA:     rediss
    HOST:       localhost.localdomain
    PORT:       6379
    DATABASE:   0
    NAME:       General  
    PASSWORD:   secure-password
```

```python
    FlaskRedisSingle( app )
```
#### Example instantiate with dictionary

```python
    config = {
        'URL': 'rediss://localhost.localdomain:6379',
        'DATABASE': 0,
        'NAME': 'General',  
        'PASSWORD': 'secure-password'
    }
    redis_instance = FlaskRedisSingle( config )
    redis_instance.init_app( app )
```

#### Example instantiate with IRedisSimpleConfig

```python
    config = IRedisSimpleConfig( URL = 'rediss://localhost.localdomain:6379',
                                 DATABASE = 0, NAME = 'General', PASSWORD = 'secure-password' )
    redis_instance = FlaskRedisSingle( config )
    redis_instance.init_app( app )
```









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
