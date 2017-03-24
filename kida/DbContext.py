'''

'''
from kida.fields import IntField, StringField, DatetimeField, DateField
import datetime
import urlparse
from common import Meta, Table

KEY_TYPE_PRIMARY = 1
KEY_TYPE_UNIQUE_KEY = 2
KEY_TYPE_UNIQUE_INDEX = 3

def create_context(dburl):
    from MySQLContext import MySQLContext
    from OracleContext import OracleContext
    urlparts = urlparse.urlparse(dburl)
    if urlparts.scheme == 'mysql':
        return MySQLContext(dburl)
    elif urlparts.scheme == 'oracle':
        return OracleContext(dburl)
    raise Exception('Not supported database scheme %s' % urlparts.scheme)


class DbContext(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self._meta = Meta()

    def save(self, tablename, row):
        pass

    def get(self, tablename, keys):
        pass

    def delete(self, tablename, keys):
        pass

    def save_batch(self, tablename, rows):
        pass

    def get_table(self, tablename):
        pass

    def set_table(self, table):
        pass

    def set_metadata(self, tablename, fields):
        pass

    def load_metadata(self, tablename):
        pass
    
class Dialect:
    def format_value_string(self, field, value):
        if isinstance(field, StringField):
            return '\'' + value.replace('\'', '\'\'') + '\'' 
        if isinstance(field, DatetimeField):
            if isinstance(value, datetime.datetime):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            if isinstance(value, datetime.date):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            return '\'' + str(value) + '\''
        if isinstance(field, DateField):
            if isinstance(value, datetime.datetime):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            if isinstance(value, datetime.date):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            return '\'' + str(value) + '\''
        return str(value)