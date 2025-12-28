import logging
import typing as t
import uuid
import redis
from pydantic import BaseModel
from flask_multi_redis.config import IRedisSimpleConfig
from flask_multi_redis.exc import RedisAlreadyConfigured

try:
    import orjson   as json

except ImportError:
    import json


__all__ = [ 'RedisCore' ]
logger = logging.getLogger( 'flask_redis.core' )


class RedisCore(object):
    """This is the Redis core class that handles all the core operations towards the Redis server.
    """
    def __init__( self, config: t.Optional[ t.Union[ dict, IRedisSimpleConfig ] ] = None,
                  strict: t.Optional[ bool ] = True,
                  prefix: t.Optional[ str ] = None,
                  name: t.Optional[ str ] = None,
                  **kwargs):
        """Constructor is used as session class for FlaskRedisSingle() and FlaskRedisMulti() classes.

        :param config:  is a dictionary or IRedisSimpleConfig interface with the following attributes
                        URL:        <url>                   Optional when SCHEMA, HOST, DATABASE are provided.
                        SCHEMA:     { redis | rediss }
                        HOST:       <host>                  The Hostname of the redis server
                        PORT:       <port>                  Optional port number of the redis server, defaults to 6379
                        DATABASE:   <db>                    The Database number
                        PREFIX:     <prefix>                Optional prefix that is used for the storage/retrieval key
                                                            data keys and for the subscriptions.
        :param strict:  is a boolean value that indicates whether to use strict mode or not.
        :param prefix:  is a string that indicates the prefix that is used for the redis subscriptions, publish and keys.
        :param name:    is a string that indicates the connection name.

        :raises:        TypeError
        """
        self._redis_client = None
        self.__pub_sub = None
        self._provider_class: redis.Redis = redis.StrictRedis if strict else redis.Redis  # noqa
        self._provider_kwargs = kwargs
        self._name = name
        if 'client_name' not in self._provider_kwargs:
            self._provider_kwargs[ 'client_name' ] = f'{self.__class__.__name__}-{config.PREFIX}-{config.DATABASE}'

        self.__prefix = prefix
        if isinstance( config, dict ):
            self._config = IRedisSimpleConfig( **config )
            if isinstance(self.__prefix, str):
                self._config.PREFIX = self.__prefix

        elif isinstance( config, ( IRedisSimpleConfig, None ) ):
            self._config = config
            if isinstance( self.__prefix, str ):
                self._config.PREFIX = self.__prefix

        else:
            raise TypeError('config is not a dict or IRedisSimpleConfig')

        return

    def setup( self, config: t.Union[ dict, IRedisSimpleConfig ] ):
        """Setup configuration for the session.

        :param config:  is a dictionary or IRedisSimpleConfig interface with the following attributes
                        URL:        <url>                   Optional when SCHEMA, HOST, DATABASE are provided.
                        SCHEMA:     { redis | rediss }
                        HOST:       <host>                  The Hostname of the redis server
                        PORT:       <port>                  Optional port number of the redis server, defaults to 6379
                        DATABASE:   <db>                    The Database number
                        PREFIX:     <prefix>                Optional prefix that is used for the storage/retrieval key
                                                            data keys and for the subscriptions.
        :return:        None
        :raises:        RedisAlreadyConfigured, TypeError
        """
        if isinstance( self._config, IRedisSimpleConfig ):
            raise RedisAlreadyConfigured()

        elif isinstance( config, dict ):
            self._config = IRedisSimpleConfig(**config)

        elif isinstance( config, IRedisSimpleConfig ):
            self._config = config

        else:
            raise TypeError( 'config is not a dict or IRedisSimpleConfig' )

        return

    def _build_url( self ) -> str:
        """Internal function to build th eRedis URL from the configuration attributes.

        Requires in the configuration at least
            'URL'
        or
            'SCHEMA'
            'HOST'
            'PORT'
            'DATABASE'

            Optional:
                'PASSWORD'
                'USERNAME'

        :returns str:           With the Redis URL
        """
        if isinstance( self._config.URL, str ):  # noqa
            return self._config.URL

        if isinstance(self._config.PASSWORD, str):
            if isinstance(self._config.USERNAME, str):
                return "{SCHEMA}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format( **self._config.model_dump() )

            return "{SCHEMA}://nobody:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format( **self._config.model_dump() )

        return "{SCHEMA}://{HOST}:{PORT}/{DATABASE}".format( **self._config.model_dump() )

    def connect( self ) -> None:
        """Performs the setup of the connection to the redis server.

        :returns:               None
        :raises:                ConnectionError
        """
        logger.info( f"connecting to redis: { self._config.HOST }:{ self._config.PORT }/{ self._config.DATABASE }" )
        self._redis_client = self._provider_class.from_url( self._build_url(), **self._provider_kwargs )
        if self._redis_client.connection is None and self._redis_client.connection_pool is None:
            raise ConnectionError( 'Redis connection failed.' )

        self._redis_client.ping()
        return

    def disconnect( self ) -> None:
        """Closes the connection to the redis server.

        :returns:               None
        """
        logger.info(f"disconnecting to redis: { self._config.HOST }:{ self._config.PORT }/{ self._config.DATABASE }")
        self._redis_client.close()
        self._redis_client = None
        return

    @classmethod
    def from_custom_provider( cls, provider, **kwargs ):
        """Special class method for instantiating a custom provider.

        :returns object:        The new Redis provider class.
        """
        assert provider is not None, "your custom provider is None, come on"
        # We never pass the app parameter here, so we can call init_app
        # ourselves later, after the provider class has been set
        instance = cls( **kwargs )
        instance.provider_class = provider
        return instance

    @property
    def Redis( self ) -> redis.Redis:
        """Returns the low level Redis instance.

        :returns redis.Redis:   The low level Redis object
        """
        return self._redis_client

    @property
    def Database(self) -> int:
        """Returns the database number associate with the current session.

        :return int:            The database number
        """
        return self._config.DB

    @property
    def Name( self ) -> str:
        """Returns the session name.

        :returns str:           The connection name of the session.
        """
        return self._name

    @property
    def Pipeline( self ):
        """Returns the redis pipeline object.

        :returns redis.Pipeline:    The redis pipeline object
        """
        return self._redis_client.pipeline()

    def _make_attribute( self, item: str ):
        """Internal method to prefix an attribute name, publish or subscription.

        :param item:        The item to be prefix attribute name, publish or subscription.
        :returns str:       The prefixed attribute name, publish or subscription
        """
        if isinstance(self._config.PREFIX, str) and not item.startswith(f"{self._config.PREFIX}-"):
            return f"{self._config.PREFIX}-{item}"

        return item

    def has( self, key: str ) -> bool:
        """Check if a key exists in the database.

        :param key:         The key to be checked for existence.
        :returns bool:      Whether the key exists or not.
        """
        return self._redis_client.exists( self._make_attribute( key ) )

    def get( self, key: str, return_type = str, encoding: str = 'utf-8' ) -> t.Any:
        """Retrieve a value from the database.

        :param key:         The key to be retrieved from the database.
        :param return_type: The return type to be returned from the database.
                            str:                    the value shall be converted to string.
                            int:                    the value shall be converted to integer.
                            dict/list:              the value shall be converted to a dictionary or list using the json decoder.
                            bytes:                  the value shall be return as bytes (as retrieved from the server).
                            pydantic Model class:   The bytes value shall be converted to a pydantic model.
        :param encoding:    The encoding to be returned from the database.
        :returns any:       data based on the 'return_type' parameter.
        :raises:            ValidationError when return_type is a pydantic model and validation fails.
                            TypeError when a invalid return type is passed.
        """
        data = self._redis_client.get( self._make_attribute( key ) )
        if data is None:
            data = None

        elif return_type is str:
            data = data.decode( encoding )

        elif return_type is int:
            data = int( data.decode( encoding ) )

        elif return_type is float:
            data = float( data.decode( encoding ) )

        elif return_type in ( dict, list ):
            data = json.loads( data )

        elif issubclass( return_type, BaseModel ):
            return_type: BaseModel
            data = return_type.model_validate_json( data )

        elif return_type is bytes:
            pass

        else:
            raise TypeError('Invalid return_type')

        logger.debug(f"fetch data on key {key} from database {self._config.DB} => {data}")
        return data

    def set( self, key: str, value: t.Any, encoding='utf-8' ) -> None:
        """Stores value in the database on the given key.

        :param key:         The key to be stored in the database.
        :param value:       The value to be stored in the database.
                            The value can be one of the following types:
                            str:            Convert the string using encoding
                            int:            Convert the integer
                            float:          Convert the float
                            bytes:          Store the bytes as-is
                            dict/list:      Convert the dictionary or list using the json encoder.
                            pydantic Model: Convert to json using the pydantic model.
        :param encoding:    The encoding to be stored in the database.
        :returns None:      None
        :raises:            TypeError when a invalid return type is passed.
        """
        key = self._make_attribute( key )
        if isinstance( value, str ):
            self._redis_client.set(key, value.encode(encoding))

        elif isinstance( value, ( int, float ) ):
            self._redis_client.set(key, str(value).encode(encoding))

        elif isinstance( value, ( dict, list ) ):
            self._redis_client.set(key, json.dumps( value ) )

        elif isinstance( value, bytes ):
            self._redis_client.set( key, value )

        elif isinstance( value, BaseModel ):
            self._redis_client.set( key, value.model_dump_json() )

        else:
            raise TypeError(f'Invalid value type {value}')

        logger.debug(f"set data on key { key } on database { self._config.DB } => { value }")
        return

    def delete( self, key: str ) -> int:
        """Delete a value from the database using the given key.

        :returns int:           result of the deletion.
                                0 = key dont exist
        """
        key = self._make_attribute( key )
        self._redis_client: redis.Redis
        if self._redis_client.exists( key ):
            logger.info(f"delete data on key { key } from database { self._config.DB }")
            return self._redis_client.delete( key )

        return 0

    def publish( self, subscription, data: t.Any, encoding: t.Optional[ str ] = 'utf-8' ) -> None:
        """Publish a message on the given subscription.

        :param subscription:    The subscription to be published.
        :param data:            The data to be published. Maybe of the following types:
                                str:            Convert to bytes using the encoding.
                                bytes:          Pass as-is.
                                int:            Convert to bytes using the encoding.
                                float:          Convert to bytes using the encoding.
                                list/dict:      Convert to JSON.
                                pydantic Model: Convert to JSON using the pydantic model.
        :param encoding:        The encoding to be returned from the database.
        :returns None:
        :raises:                TypeError when data is invalid type.
        """
        subscription = self._make_attribute( subscription )
        if isinstance( data, str ):
            data = data.encode( encoding )

        elif isinstance( data, bytes ):
            pass

        elif isinstance( data, ( int, float ) ):
            data = str( data ).encode( encoding )

        elif isinstance( data, BaseModel ):
            data = data.model_dump_json()

        elif isinstance( data, ( dict, list ) ):
            data = json.dumps( data )

        else:
            raise TypeError( 'Invalid data type' )

        logger.info( f"Publish on { subscription } data { data }")
        self._redis_client.publish( subscription, data )
        return

    def make_pubsub( self ) -> redis.client.PubSub:
        """Return a pub/sub object detached from the RedisCore class.

        :returns:               redis.client.PubSub object
        """
        return self._redis_client.pubsub()

    def subscribe( self, *subscription ) -> redis.client.PubSub:
        """Subscribe to a given subscription(s).

        :param subscription:    The subscription to be subscribed.
        :returns:               redis.client.PubSub object
        """
        if self.__pub_sub is None:
            self.__pub_sub = self._redis_client.pubsub()

        subscriptions = [ self._make_attribute( sub ) for sub in subscription ]
        logger.debug( f"Subscribe to { subscriptions }")
        self.__pub_sub.subscribe( *subscriptions )
        return self.__pub_sub

    @property
    def PusSub( self ):
        """Return a pub/sub object attached to the RedisCore class.
        The object maybe created by the subscribe() or publish() calls

        :returns:               redis.client.PubSub object
        """
        if not isinstance( self.__pub_sub, redis.client.PubSub ):
            self.__pub_sub = self._redis_client.pubsub()

        return self.__pub_sub

    def unsubscribe( self, pub_sub: t.Union[ redis.client.PubSub, str ] ) -> None:  # noqa
        """Unsubscribe from a given subscription or pub/sub object.

        :param pub_sub:        The pub/sub object to be unsubscribed, or a subscription name.

        :returns:              None
        """
        if isinstance( pub_sub, redis.client.PubSub ):
            logger.debug( f"Unsubscribe { pub_sub.channels }" )
            self.__pub_sub = None
            return pub_sub.unsubscribe()

        logger.debug( f"Unsubscribe { pub_sub }" )
        self.PusSub.unsubscribe( pub_sub )
        self.__pub_sub = None
        return

    @staticmethod
    def wait_for_reply( pub_sub: redis.client.PubSub, return_type=str, timeout: int = 30, encoding='utf-8',
                        allow_error: t.Optional[ bool ] = False ) -> t.Any:
        """Wait on a subscription for a reply message. The reply message is convert accordingly to 'return_type'

        :param pub_sub:         The pub/sub object to wait on.
        :param return_type:     The return type of the reply message.
        :param timeout:         The timeout in seconds.
        :param encoding:        The encoding to be returned from the database.
        :param allow_error:     The allow_error to be returned from the database.

        :returns any:           Depending on the parameter 'return_type' or None on a timeout if allow_error is True.
        :raises:                TypeError, TimeoutError
        """
        logger.debug(f"Waiting {pub_sub.channels} for {timeout} secs on data")
        reply = {}
        while reply.get('type', 'init') != 'message':
            reply = pub_sub.get_message(timeout=timeout)
            if reply is None:  # This is a timeout
                reply = {}
                break

        if reply.get('type', 'init') != 'message':
            if allow_error:
                if return_type is dict:
                    return {}

                elif return_type is list:
                    return []

                return None

            else:
                raise TimeoutError()

        reply = reply.get( 'data' )
        logger.info( f"Reply { pub_sub.channels } with {reply}" )
        if issubclass( return_type, BaseModel ):
            return return_type.model_validate_json( reply )                 # noqa

        elif return_type is str:
            return reply.decode(encoding)

        elif return_type is int:
            return int( reply.decode(encoding ) )

        elif return_type in ( dict, list ):
            return json.loads( reply )

        elif return_type is bytes:
            pass

        else:
            raise TypeError('Invalid return_type')

        return reply

    def publish_and_wait_reply( self, subscription: str, data: t.Union[ dict, BaseModel ], return_type=str,
                                timeout: int = 30, encoding: str = 'utf-8',
                                allow_error: t.Optional[ bool ] = False ) -> t.Any:
        """Sends a message on the subscription, creates a unique reply subscription and wait for a reply message.
        The reply message is convert accordingly to 'return_type'

        :param subscription:    The subscription to send the message on.
        :param data:            The data to be sent.
        :param return_type:     The return type of the reply message.
        :param timeout:         The timeout in seconds.
        :param encoding:        The encoding to be returned from the database.
        :param allow_error:     The allow_error to be returned from the database.

        :returns any:           Depending on the parameter 'return_type' or None on a timeout.
        :raises:                TypeError, TimeoutError

        """
        reply_subscription = uuid.uuid4().hex
        pub_sub = self.subscribe( reply_subscription )
        if isinstance(data, BaseModel):
            data.reply_to = self._make_attribute(reply_subscription)
            data = data.model_dump()

        elif isinstance(data, dict):
            data[ 'reply_to' ] = self._make_attribute(reply_subscription)

        else:
            raise TypeError( 'Invalid type of data' )

        logger.info( f"Request: {data}" )
        self.publish(subscription, data, encoding)
        reply = self.wait_for_reply( pub_sub, return_type, timeout, encoding, allow_error )
        self.unsubscribe( pub_sub )
        return reply

    def ping( self ) -> None:
        """Send a Redis ping on the connection.

        :returns:           None
        """
        self.Redis.ping()
        return
