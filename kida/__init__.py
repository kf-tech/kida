__version__ = '0.0.9'

from kida.DbContext import DbContext, create_context
from kida.MySQLContext import MySQLContext
from kida.OracleContext import OracleContext

connect = create_context

from kida.fields import IntField, StringField, DatetimeField, DateField, DecimalField, BinaryField
from kida.DbContext import KEY_TYPE_PRIMARY, KEY_TYPE_UNIQUE_KEY, KEY_TYPE_UNIQUE_INDEX