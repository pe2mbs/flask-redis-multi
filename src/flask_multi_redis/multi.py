import threading
import typing as t
import logging
import uuid
import orjson
import redis
import redis.client
from flask import Flask
from flask_multi_redis.core import RedisCore
from flask_multi_redis.config import IRedisSimpleConfig
from flask_multi_redis.exc import RedisDatabaseNotAvailable, FlaskAppNotProvided


__all__ = [ 'FlaskRedisMulti' ]
logger = logging.getLogger( 'flask_redis.multi' )


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