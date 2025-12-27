import threading
import typing as t
import logging
import uuid
import orjson
import redis
import redis.client
from flask import Flask
from pydantic import BaseModel, Field
from flask_multi_redis.exc import RedisAlreadyConfigured, RedisDatabaseNotAvailable, NoSubscriptionPubisher, \
    FlaskAppNotProvided, InvalidRedisDatabase

__all__ = [ 'RedisCore', 'FlaskRedisSingle', 'FlaskRedisMulti', 'IRedisSimpleConfig', 'IFlaskRedisMultiConfig' ]
logger = logging.getLogger( 'flask_redis' )


class IRedisSimpleConfig( BaseModel ):
    URL:        t.Optional[ str ]   =   Field( None )
    SCHEMA:     t.Optional[ t.Literal[ 'redis' , 'rediss' ] ] = Field( 'redis' )
    HOST:       t.Optional[ str ] = Field( None )
    PORT:       t.Optional[ int ] = Field( 6379 )
    DATABASE:   t.Optional[ int ] = Field( 0 )
    NAME:       t.Optional[ str ] = Field( None )
    USERNAME:   t.Optional[ str ] = Field( None )
    PASSWORD:   t.Optional[ str ] = Field( None )
    PREFIX:     t.Optional[ str ] = Field( None )


class IFlaskRedisMultiConfig( BaseModel ):
    DATABASES:          t.Dict[ str, int ]
    HOST:               str
    PORT:               t.Optional[ int ]               = Field( 6379 )
    MAX_CONNECTIONS:    t.Optional[ int ]               = Field( 10 )
    PASSWORD:           t.Optional[ str ]               = Field( None )
    USERNAME:           t.Optional[ str ]               = Field( None )


class RedisCore( object ):
    def __init__( self, config: t.Optional[ t.Union[ dict, IRedisSimpleConfig ] ] = None,
                        strict: t.Optional[ bool ] = True,
                        prefix: t.Optional[ str ] = None,
                        name: t.Optional[ str ] = None,
                        **kwargs ):
        self._redis_client      = None
        self.__pub_sub          = None
        self._provider_class: redis.Redis = redis.StrictRedis if strict else redis.Redis   # noqa
        self._provider_kwargs   = kwargs
        self._name              = name
        if 'client_name' not in self._provider_kwargs:
            self._provider_kwargs[ 'client_name' ] = f'{ self.__class__.__name__ }-{ config.PREFIX }-{ config.DATABASE }'
        """
            config: is a dictionary with the following attributes
                URL:        <url>                   Optional when SCHEMA, HOST, DATABASE are provided. 
                SCHEMA:     { redis | rediss }
                HOST:       <host>                  The Hostname of the redis server
                PORT:       <port>                  Optional port number of the redis server, defaults to 6379
                DATABASE:   <db>                    The Database number
                PREFIX:     <prefix>                Optional prefix that is used for the storage/retrieval key data keys
                                                    and for the subscriptions. 
            
            
        """
        self.__prefix = prefix
        if isinstance( config, dict ):
            self._config = IRedisSimpleConfig( **config )
            if isinstance( self.__prefix , str ):
                self._config.PREFIX = self.__prefix

        elif isinstance( config, ( IRedisSimpleConfig, None ) ):
            self._config = config
            if isinstance( self.__prefix , str ):
                self._config.PREFIX = self.__prefix

        else:
            raise TypeError( 'config is not a dict or IRedisSimpleConfig' )

        return

    def setup( self, config: t.Union[ dict, IRedisSimpleConfig ] ):
        if isinstance( self._config, IRedisSimpleConfig ):
            raise RedisAlreadyConfigured()

        elif isinstance( config, dict ):
            self._config = IRedisSimpleConfig( **config )

        elif isinstance( config, IRedisSimpleConfig ):
            self._config = config

        else:
            raise TypeError( 'config is not a dict or IRedisSimpleConfig' )

        return

    def _build_url( self ) -> str:
        if isinstance( self._config.URL, str ):   # noqa
            return self._config[ "URL" ]

        if isinstance( self._config.PASSWORD, str ):
            if isinstance(self._config.USERNAME, str ):
                return "{SCHEMA}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format(**self._config.model_dump())

            return "{SCHEMA}://nobody:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format(**self._config.model_dump())

        return "{SCHEMA}://{HOST}:{PORT}/{DATABASE}".format( **self._config.model_dump() )

    def connect( self ) -> None:
        logger.info( f"connecting to redis: { self._config.HOST }, { self._config.PORT }, { self._config.DATABASE }" )
        self._redis_client = self._provider_class.from_url( self._build_url(), **self._provider_kwargs )
        if self._redis_client.connection is None and self._redis_client.connection_pool is None:
            raise ConnectionError( 'Redis connection failed.' )

        self._redis_client.ping()
        return

    def disconnect( self ) -> None:
        logger.info( f"disconnecting to redis: { self._config.HOST }, { self._config.PORT }, { self._config.DATABASE }" )
        self._redis_client.close()
        self._redis_client = None
        return

    @classmethod
    def from_custom_provider( cls, provider, **kwargs):
        assert provider is not None, "your custom provider is None, come on"
        # We never pass the app parameter here, so we can call init_app
        # ourselves later, after the provider class has been set
        instance = cls( **kwargs )
        instance.provider_class = provider
        return instance

    @property
    def Redis( self ) -> redis.Redis:
        return self._redis_client

    @property
    def Database( self ) -> int:
        return self._config.DB

    @property
    def Name( self ) -> str:
        return self._name

    @property
    def Pipeline( self ):
        return self._redis_client.pipeline()

    def _make_attribute( self, item: str ):
        if isinstance( self._config.PREFIX, str ) and not item.startswith( f"{ self._config.PREFIX }-"):
            return f"{ self._config.PREFIX }-{ item }"

        return item

    def has( self, key: str ) -> bool:
        return self._redis_client.exists( self._make_attribute( key ) )

    def get( self, key, return_type = str, encoding: str = 'utf-8' ) -> t.Any:
        data = self._redis_client.get( self._make_attribute( key ) )
        if data is None:
            return None

        elif return_type is str:
            return data.decode( encoding )

        elif return_type is int:
            return int( data.decode( encoding ) )

        elif return_type is dict or return_type is list:
            return orjson.loads( data )

        elif return_type is bytes:
            pass

        else:
            raise TypeError( 'Invalid return_type' )

        logger.debug( f"fetch data on key { key } from database { self._config.DB } => { data }" )
        return data

    def set( self, key: str, value: t.Any, encoding = 'utf-8' ) -> None:
        key = self._make_attribute( key )
        if isinstance( value, str ):
            self._redis_client.set( key, value.encode( encoding ) )

        elif isinstance( value, int ):
            self._redis_client.set(key, str( value ).encode( encoding ) )

        elif isinstance( value, (dict,list) ):
            self._redis_client.set( key, orjson.dumps( value ) )

        elif isinstance( value, bytes ):
            self._redis_client.set( key, value )

        else:
            raise TypeError( f'Invalid value type {value}' )

        logger.debug( f"set data on key { key } on database { self._config.DB } => { value }" )
        return

    def delete( self, key: str ) -> int:
        key = self._make_attribute( key )
        self._redis_client: redis.Redis
        if self._redis_client.exists( key ):
            logger.info( f"delete data on key { key } from database { self._config.DB }" )
            return self._redis_client.delete( key )

        return 0

    def publish( self, subscription, data: t.Any, encoding: t.Optional[ str ] = 'utf-8' ) -> None:
        subscription = self._make_attribute( subscription )
        if isinstance( data, str ):
            data = data.encode( encoding )

        elif isinstance( data, bytes ):
            pass

        elif isinstance( data, int ):
            data = str( data ).encode( encoding )

        elif isinstance( data, BaseModel ):
            data = data.model_dump_json()

        elif isinstance( data, ( dict, list ) ):
            data = orjson.dumps( data )

        else:
            raise TypeError('Invalid data type' )

        logger.info( f"Publish on { subscription } data { data }" )
        self._redis_client.publish( subscription, data )
        return

    def make_pubsub( self ) ->  redis.client.PubSub:
        return self._redis_client.pubsub()

    def subscribe( self, *subscription ) -> redis.client.PubSub:
        if self.__pub_sub is None:
            self.__pub_sub = self._redis_client.pubsub()

        subscriptions = [ self._make_attribute( sub ) for sub in subscription ]
        logger.debug( f"Subscribe to {subscriptions}")
        self.__pub_sub.subscribe( *subscriptions )
        return self.__pub_sub

    @property
    def PusSub( self ):
        if not isinstance(self.__pub_sub, redis.client.PubSub ):
            self.__pub_sub = self._redis_client.pubsub()

        return self.__pub_sub

    def unsubscribe( self, pub_sub: t.Union[ redis.client.PubSub, str ] ) -> None:              # noqa
        if isinstance( pub_sub, redis.client.PubSub ):
            logger.debug( f"Unsubscribe {pub_sub.channels}" )
            self.__pub_sub = None
            return pub_sub.unsubscribe()

        logger.debug( f"Unsubscribe {pub_sub}" )
        self.PusSub.unsubscribe( pub_sub )
        self.__pub_sub = None
        return

    @staticmethod
    def wait_for_reply( pub_sub: redis.client.PubSub, return_type = str, timeout: int = 30, encoding = 'utf-8' ) -> t.Any:
        logger.debug( f"Waiting { pub_sub.channels } for { timeout } secs on data" )
        reply = {}
        while reply.get( 'type', 'init' ) != 'message':
            reply = pub_sub.get_message( timeout = timeout )
            if reply is None:   # This is a timeout
                reply = {}
                break

        if reply.get( 'type', 'init' ) != 'message':
            if return_type is dict:
                return { 'result': False, 'message': 'Timeout waiting for reply' }

            elif return_type is list:
                return []

            return None

        reply = reply.get( 'data' )
        logger.info(f"Reply {pub_sub.channels} with {reply}")
        if issubclass( return_type, BaseModel ):
            return return_type.model_validate_json( reply )

        elif return_type is str:
            return reply.decode( encoding )

        elif return_type is int:
            return int( reply.decode( encoding ) )

        elif return_type is dict or return_type is list:
            return orjson.loads( reply )

        elif return_type is bytes:
            pass

        else:
            raise TypeError( 'Invalid return_type' )

        return reply

    def publish_and_wait_reply( self, subscription: str, data: t.Any, return_type = str, timeout: int = 30, encoding: str = 'utf-8' ) -> t.Any:
        reply_subscription = uuid.uuid4().hex
        pub_sub = self.subscribe( reply_subscription )
        if isinstance( data, BaseModel ):
            data.reply_to = self._make_attribute(reply_subscription)
            data = data.model_dump()

        elif isinstance( data, dict ):
            data[ 'reply_to' ] = self._make_attribute( reply_subscription )

        logger.info( f"Request: { data }" )
        self.publish( subscription, data, encoding )
        reply = self.wait_for_reply( pub_sub, return_type, timeout, encoding )
        self.unsubscribe( pub_sub )
        return reply

    def ping( self ):
        self.Redis.ping()
        return


class FlaskRedisSingle( RedisCore ):
    def __init__( self, app: t.Optional[ t.Union[ 'Flask', dict, IRedisSimpleConfig ] ] = None,
                        strict: t.Optional[ bool ] = True,
                        config_element: t.Optional[ str ] = "REDIS",
                        prefix: t.Optional[ str ] = None, **kwargs ):
        """Create a single Redis instance for one database.

        :param app:             Flask application instance.
        :param strict:          To use RedisStrict or just Redis class
        :param config_element:  The configuration key to use from the Flask configuration.
        :param prefix:          The prefix string to use when performing Redis actions (has,get,set,delete,publish,subscribe).
        :param kwargs:          Extra keyword arguments to pass to the Redis client.

        # Basic configuration:
            REGIS:
                URL:        <url>
                SCHEMA:     { redis | rediss }
                HOST:       <host>
                PORT:       <port>
                DATABASE:   <db>
                PREFIX:     <prefix>

        """
        self.__config_element = config_element
        if app is None:
            super().__init__( app, strict, prefix, **kwargs)

        elif isinstance( app, Flask ):
            cfg = app.config.get( config_element )
            kwargs = {}
            cfg = IRedisSimpleConfig( **cfg )
            if isinstance( prefix, str ):
                cfg.PREFIX = prefix

            super().__init__( cfg, strict, **kwargs )

        elif isinstance( app, dict ):
            cfg = IRedisSimpleConfig( **app )
            if isinstance( prefix, str ):
                cfg.PREFIX = prefix

            super().__init__( cfg, strict, **kwargs )

        else:
            raise TypeError( f'Invalid app type { app }' )

        if isinstance( app, Flask ):
            self.init_app( app )

        return

    @classmethod
    def from_custom_provider( cls,  provider: t.Union[ redis.Redis, redis.StrictRedis ],
                                    app: t.Optional[ t.Union[ Flask, IRedisSimpleConfig, dict ] ] = None,
                                    **kwargs ):
        assert provider is not None, "your custom provider is None, come on"
        # We never pass the app parameter here, so we can call init_app
        # ourselves later, after the provider class has been set
        instance = cls( app, **kwargs )
        instance.provider_class = provider
        return instance

    def init_app( self, app: Flask, **kwargs ):
        self._provider_kwargs.update( kwargs )
        if not isinstance( self._config, IRedisSimpleConfig ):
            self._config = app.config.get( self.__config_element )
            if isinstance( self.__prefix , str ):
                self._config.PREFIX = self.__prefix

        self.connect()
        app.extensions['redis'] = self
        return

    def getRedis( self, database: t.Union[ int, str ] ) -> RedisCore:
        if isinstance( database, str ) and self._config.NAME == database:
            return self

        elif not isinstance( database, int ):
            raise InvalidRedisDatabase()

        if database != self._config.DATABASE:
            raise InvalidRedisDatabase()

        return self


class RedisSession( object ):
    """Storage object for keeping track of the database id, name and session

    """
    def __init__( self, name: str, database: int, session: RedisCore ):
        self.name       = name
        self.database   = database
        self.session    = session
        return


class FlaskRedisMulti( object ):
    def __init__(self,  app: t.Optional[ Flask ] = None,
                        strict: t.Optional[ bool ] = True,
                        config_element: t.Optional[ str ] = "REDIS",
                        prefix: t.Optional[ str ] = None,
                        **kwargs ):
        """Instantiate a Flask Redis client for multiple databases..

        :param app:             Flask application instance.
        :param strict:          To use RedisStrict or just Redis class
        :param config_element:  The configuration key to use from the Flask configuration.
        :param prefix:          The prefix string to use when performing Redis actions (has,get,set,delete,publish,subscribe).
        :param kwargs:          Extra keyword arguments to pass to the Redis client.

        Basic configuration:
            REDIS:
                DATABASES:
                    0:  GENERAL
                    1:  WEBAPP_DB
                    2:  ANSWERING_MACHINES
                    3:  LOAD_TESTING
                    4:  MAIL_SERVICE
                HOST:                 localhost
                PORT:                 6379
                MAX_CONNECTIONS:      100
                PASSWORD:             VerySecurePassword
                USERNAME:
        """
        self.__lock = threading.RLock()
        self.__provider_class: redis.Redis = redis.StrictRedis if strict else redis.Redis  # noqa
        self.__provider_kwargs = kwargs
        self.__config_element = config_element
        self.__prefix = prefix
        self.__config = None
        self.__round_robin = 0
        self.__db_dict = {}
        self.__databases: t.Dict[ t.Union[ int, str ], RedisSession ] = {}
        if isinstance( app, Flask ):
            self.init_app( app )

        return

    def init_app( self, app: 'Flask', **kwargs ) -> None:
        """Initialize the Flask Redis client.

        :param app:             The flask class instance.
        :param kwargs:          The kwargs passed to the redis client.
        :return:                None

        """
        self.__config = app.config.get( self.__config_element )
        if isinstance( app, Flask ):
            self.__db_dict.update(  self.__config.get( 'DATABASES' ) )
            for name, database in self.__config.get( 'DATABASES' ).items():
                self.__db_dict[ database ] = name
                cfg = IRedisSimpleConfig(   DATABASE    = database,
                                            SCHEMA      = self.__config.get( 'SCHEMA', 'redis' ),
                                            HOST        = self.__config.get( 'HOST' ),
                                            PORT        = self.__config.get( 'PORT', 6379 ),
                                            PASSWORD    = self.__config.get( 'PASSWORD' ),
                                            USERNAME    = self.__config.get( 'USERNAME' ),
                                            NAME        = name,
                                            PREFIX      = self.__config.get( 'PREFIX' ) )               # noqa
                session = RedisCore( cfg, **kwargs, name = name )
                session.connect()
                node = RedisSession( name = name, database = database, session = session )
                self.__databases[ name ] = node
                self.__databases[ database ] = node

            app.extensions[ 'redis' ] = self

        else:
            raise FlaskAppNotProvided()

        return

    @property
    def Databases( self ) -> t.Dict[ t.Union[ int, str ], RedisSession ]:
        """Return a dictionary with database ids/database names as keys. And value the resid session.

        """
        return self.__databases

    @property
    def DatabasesByName(self) -> t.Dict[ str, RedisSession ]:
        return { name: session for name, session in self.__databases.items() if isinstance( name, str ) }

    @property
    def DatabasesById(self) -> t.Dict[ int, RedisSession ]:
        return { name: session for name, session in self.__databases.items() if isinstance( name, int ) }

    def _build_url( self, db: int ) -> str:
        """Create the URL string to connect to the Redis database.

        :param db:              database number
        :return:                url string
        """
        if isinstance( self.__config.PASSWORD, str ):
            if isinstance(self.__config.USERNAME, str ):
                return "{SCHEMA}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{db}".format(**self.__config.model_dump(), db = db )

            return "{SCHEMA}://{PASSWORD}@{HOST}:{PORT}/{db}".format( **self.__config.model_dump(), db = db )

        return "{SCHEMA}://{HOST}:{PORT}/{db}".format( **self.__config.model_dump(), db = db )


    def _check_database_id( self, database: t.Union[ int, str ] ) -> int:
        if isinstance( database, str ):
            database = self.__db_dict[ database ]

        if database > len( self.__databases ):
            raise RedisDatabaseNotAvailable()

        return database

    def get( self, database: t.Union[ int, str ], key, return_type=str, encoding: str = 'utf-8') -> t.Any:
        """Get a key from the database.

        :param database:        database number
        :param key:             key to get
        :param return_type:     return type to return of the key value
        :param encoding:        encoding to use for value
        :return:                the value in the requested encoding.
        :raises                 RedisDatabaseNotAvailable: if the database doesn't exist

        """
        return self.__databases[ self._check_database_id( database ) ].session.get( key )

    def has( self, database: t.Union[ int, str ], key: str ) -> bool:
        """Check if a key is on the database.

        :param database:        database number
        :param key:             key to check
        :returns:               true if the key exists, false otherwise
        :raises                 RedisDatabaseNotAvailable: if the database doesn't exist

        """
        return self.__databases[ self._check_database_id( database ) ].session.has( key )

    def set( self, database: t.Union[ int, str ], key: str, value: t.Any, encoding = 'utf-8' ) -> None:
        """Set a value on a key on the database.

        :param database:        database number
        :param key:             key to set
        :param value:           value to set
        :param encoding:        encoding to use for the value
        :returns:               None
        :raises                 RedisDatabaseNotAvailable: if the database doesn't exist

        """
        self.__databases[ self._check_database_id( database ) ].session.set( key )
        return

    def delete( self, database: t.Union[ int, str ], key: str ) -> None:
        """Delete a key from the database.

        :param database:        database number
        :returns:               None
        :raises                 RedisDatabaseNotAvailable: if the database doesn't exist

        """
        self.__databases[ self._check_database_id( database ) ].session.delete( key )
        return

    def getRedis( self, database: t.Union[ int, str ] ) -> RedisCore:
        """Gets the actual Redis class of the specific database.

        :param database:        database number
        :returns:               RedisCore class instance
        :raises                 RedisDatabaseNotAvailable: if the database doesn't exist
        """
        return self.__databases[ self._check_database_id( database ) ].session

    def _get_node( self ) -> RedisCore:
        """Get the next redis node, to balance the subscribes

        """
        session_list = list( self.__databases.values() )
        node = session_list[ self.__round_robin ].session
        self.__lock.acquire()
        self.__round_robin += 1
        if self.__round_robin >= len( session_list ):
            self.__round_robin = 0

        self.__lock.release()
        return node

    def publish(self, subscription: str, data: t.Any, encoding: t.Optional[str] = 'utf-8') -> None:
        """Publish a message to a subscription

        :param subscription:        Redis subscription
        :param data:                Message to publish
        :param encoding:            Encoding of the message
        :return:                    None
        """
        self._get_node().publish( subscription, data, encoding )
        pass

    def subscribe( self, *subscription ) -> redis.client.PubSub:
        """Subscribe to one os more subscriptions

        :param subscription:        One of more Redis subscription(s)
        :return:                    The Redis PubSub object
        """
        return self._get_node().subscribe( *subscription )

    def unsubscribe( self, pub_sub: redis.client.PubSub ) -> None:              # noqa
        """Unsubscribe from all active subscriptions on the pub_sub or a single subscription.

        :param pub_sub:     A PubSub object created with subscribe()
        :return:            None
        """
        # get the connection where the pubsub is registered on, and unsubscribe
        for node in self.__databases.values():
            node: RedisSession
            if node.session.PusSub == pub_sub:
                node.session.unsubscribe( pub_sub )
                return

        logger.warning( "Seems to have no connection" )
        return

    @staticmethod
    def wait_for_reply( pub_sub: redis.client.PubSub, return_type = str, timeout: int = 30, encoding = 'utf-8' ) -> t.Any:
        """Wait for a response on a pubsub subscription.

        :param pub_sub:         PubSub instance
        :param return_type:     Return type of the response data
        :param timeout:         Timeout for waiting response
        :param encoding:        The encoding of the response data
        :return:                The response data according to encoding
        :raises                 TypeError on invalid return type
        """
        reply = pub_sub.get_message( timeout = timeout )
        if return_type is str:
            return reply.decode( encoding )

        elif return_type is int:
            return int( reply.decode( encoding ) )

        elif return_type is dict or return_type is list:
            return orjson.loads( reply )

        elif return_type is bytes:
            pass

        else:
            raise TypeError( 'Invalid return_type' )

        return reply

    def publish_and_wait_reply( self, subscription: str, data: t.Any, return_type = str, timeout: int = 30, encoding: str = 'utf-8' ) -> t.Any:
        """This publishes a message on the redis server, and wait for s response.

        :param subscription:    The subscription to publish on
        :param data:            The data to publish
        :param return_type:     The return type of the response data
        :param timeout:         The timeout to wait for a response
        :param encoding:        The encoding of the request data and response data
        :return:                The response data according to encoding
        """
        node = self._get_node()
        reply_subscription = subscription + '_' + uuid.uuid4().hex
        pub_sub = node.subscribe( reply_subscription )
        node.publish( subscription, data, encoding )
        reply = node.wait_for_reply( pub_sub, return_type, timeout, encoding )
        node.unsubscribe( pub_sub )
        return reply

    def ping( self ):
        self.__databases[ self.__round_robin ].session.ping()
        return