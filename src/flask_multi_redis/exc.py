
__all__ = [
    'RedisAlreadyConfigured',
    'RedisDatabaseNotAvailable',
    'NoSubscriptionPubisher',
    'FlaskAppNotProvided',
    'InvalidRedisDatabase',
]

class RedisAlreadyConfigured(Exception):
    pass


class RedisDatabaseNotAvailable( Exception ):
    pass


class NoSubscriptionPubisher( Exception ):
    pass


class FlaskAppNotProvided( Exception ):
    pass


class InvalidRedisDatabase( Exception ):
    pass
