import typing as t
import logging
import redis
from flask_multi_redis.config import IRedisSimpleConfig
from flask_multi_redis.core import RedisCore
from flask_multi_redis.exc import InvalidRedisDatabase
from flask import Flask


__all__ = [ 'FlaskRedisSingle' ]
logger = logging.getLogger( 'flask_redis.single' )



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
