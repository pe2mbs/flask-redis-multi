import typing as t
from pydantic import BaseModel, Field

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
