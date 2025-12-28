from flask_multi_redis.multi import FlaskRedisMulti
from flask_multi_redis.single import FlaskRedisSingle
from flask_multi_redis.core import RedisCore
from flask_multi_redis.config import IFlaskRedisMultiConfig, IRedisSimpleConfig


__version__ = "1.0.0"

__title__ = "flask-redis"
__description__ = "A flask extension to communicate with REDIS from a flask application"
__url__ = "https://github.com/pe2mbs/flask-redis/"
__uri__ = __url__

__author__ = "Marc Bertens"
__email__ = "m.bertens@pe2mbs.nl"

__license__ = "GPL-2.0-only"
__copyright__ = "Copyright (c) 2025 Marc Bertens"

__all__ = [ 'FlaskRedisMulti', 'FlaskRedisSingle', 'RedisCore', 'IFlaskRedisMultiConfig', 'IRedisSimpleConfig' ]
