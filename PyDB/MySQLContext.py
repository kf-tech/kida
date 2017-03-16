'''
'''
from PyDB import DbContext
from PyDB.fields import IntField, StringField, DatetimeField, DateField, DecimalField, BinaryField
from DbContext import Dialect
import logging
import collections
from .exceptions import *
from itertools import groupby
import urlparse
from common import Meta, Table

logger = logging.getLogger(__name__)

class MySQLContext(DbContext):
    '''
    classdocs
    '''

    def __init__(self,constr=None,  *args, **kwargs):
        '''
        Constructor
        '''
        import MySQLdb
        from _mysql_exceptions import ProgrammingError, OperationalError
        self._metadata = {}
        self._realtablename = {}

        if constr is not None and isinstance(constr, dict):
            params = constr.copy()
            params.update(kwargs)
            self.cnx = MySQLdb.connect(**params)
        elif constr is not None:
            urlparts = urlparse.urlparse(constr)
            username = urlparts.username or ''
            password = urlparts.password or ''
            host = urlparts.hostname
            port = urlparts.port
            dbname = urlparts.path.lstrip('/')
            other_params = urlparse.parse_qs(urlparts.query)
            other_params.update(kwargs)
            self.cnx = MySQLdb.connect(host=host, user=username, passwd = password, db=dbname,
                                       cursorclass=MySQLdb.cursors.DictCursor, **other_params)
        else:
            self.cnx = MySQLdb.connect(*args, cursorclass=MySQLdb.cursors.DictCursor, **kwargs)

        self.cursor = self.cnx.cursor(MySQLdb.cursors.DictCursor)
        self.dialect = MySQLDialect()
    
    def execute_sql(self, sql, params=None, dict_cursor=False):
        import MySQLdb
        from _mysql_exceptions import ProgrammingError, OperationalError
        logger.debug(sql)
        try:
            #cursor = self.cursor
            if dict_cursor:
                cursor = self.cnx.cursor(MySQLdb.cursors.DictCursor)
            else:
                cursor = self.cnx.cursor()
            cursor.execute(sql, params)
            return cursor
        except OperationalError:
            # if not self.cnx.is_connected():
                # self.cnx.connect()
                # cursor = self.cursor = self.cnx.cursor()
                # cursor.execute(sql, params)
                # logging.debug("connection reconnected")
                # return cursor
            raise

    def _save(self, tablename, data):
        table_metadata = self._metadata[tablename]
        data = data.copy()
        for field in data.keys():
            if field not in table_metadata.keys():
                del data[field]

        fields = ','.join(data.keys())
        values = ','.join(['%({0})s'.format(key) for key in data.keys()])
        sql = 'insert into ' + self._realtablename[tablename] + ' (' + fields + ') values (' + values + ')'
        logger.debug(sql)
        return self.execute_sql(sql, data)

    def save(self, tablename, data):
        self.save_or_update(tablename, data)
    
    def update(self, tablename, data):
        table_metadata = self._metadata[tablename]
        key_fields = []
        for field in table_metadata.values():
            if field.is_key:
                key_fields.append(field)
                
        data = data.copy()
        for key in data.keys():
            if key not in table_metadata:
                del data[key]

        sql = """update """ + self._realtablename[tablename] + " set "
        
        sql += ','.join(['{0}=%({0})s'.format(field_name) for field_name in data])
        key_condition = ' and'.join([' {0}=%({0})s '.format(key.name) for key in key_fields])
        sql += " where " + key_condition
        logger.debug(sql)
        return self.execute_sql(sql, data)
    
    def save_or_update(self, tablename, data):
        table_metadata = self._metadata[tablename]
        key_fields = []
        for field in table_metadata.values():
            if field.is_key:
                key_fields.append(field)
               
        key_signed = False  # find whether key field is specified in data 
        for key_field in key_fields:
            if key_field.name in data.keys():
                key_signed = True
                
        if key_signed:
            if self.exists_key(tablename, data):
                return self.update(tablename, data)
        else:
            raise TableKeyNotSpecified()

        return self._save(tablename, data)
    
    def get(self, tablename, keys=None):
        sql = 'select '
        table_metadata = self._metadata[tablename]
        sql_field = ','.join([field for field in table_metadata])
        sql += sql_field + ' from ' + self._realtablename[tablename]
        key_fields = filter(lambda x: x.is_key, table_metadata.values())
        
        if keys is not None:
            key_condition = 'and'.join([' %s = %s ' % (key.name, self.dialect.format_value_string(key, keys[key.name]) ) for key in key_fields])
            sql += ' where ' + key_condition

        logger.debug(sql)

        cursor = self.execute_sql(sql, dict_cursor=True)
        results = cursor.fetchall()
        if len(results) == 0:
            return None
        if len(results) ==1:
            return results[0]
        else:
            raise Exception("More than one rows with the key fetched")

    def exists_key(self, tablename, keys):
        sql = 'select count(*)'
        table_metadata = self._metadata[tablename]
        sql += ' from `' + self._realtablename[tablename] + '`'
        key_fields = filter(lambda x: x.is_key, table_metadata.values())
        
        if keys is not None:
            #key_condition = 'and'.join([' %s = %s ' % (key.name, self.dialect.format_value_string(key, keys[key.name]) ) for key in key_fields])
            key_condition = ' and '.join([' {0} = %({0})s '.format(key.name) for key in key_fields])
            sql += ' where ' + key_condition

        logger.debug(sql)
        cursor = self.execute_sql(sql, keys)
        ret = cursor.fetchone()
        if ret[0] > 0:
            return True
        return False
                
    def commit(self):
        self.cnx.commit()

    def load_table_metadata(self, tablename):
        is_primary = True
        sql = 'show tables'
        cursor = self.execute_sql(sql)
        tables = cursor.fetchall()
        tables = {x[0].upper(): x[0] for x in tables}
        if not tables.has_key(tablename.upper()):
            logger.error("Table '%s' doesn't exist" % tablename)
            return None
        self._realtablename[tablename] = tables[tablename.upper()]

        sql = 'show columns from ' + self._realtablename[tablename]
        try:
            cursor = self.execute_sql(sql)
        except ProgrammingError as e:
            return None

        field_list = collections.OrderedDict()
        fields = cursor.fetchall()
        fields = map(lambda x: {
            'Field': x[0],
            'Type': x[1],
            'Null': x[2],
            'Key': x[3],
            'Default': x[4],
            'Extra': x[5]
        }, fields)
        logger.debug(fields)
        sql = 'show index from ' + self._realtablename[tablename] + " where Non_unique = 0"
        cursor = self.execute_sql(sql)
        keys = cursor.fetchall()
        keys = sorted(
            keys,
            cmp=lambda x, y: 1 if x[2] == 'PRIMARY' else (-1 if y[2] == 'PRIMARY' else cmp(x[2], y[2])),
            reverse=is_primary)
        group = groupby(keys, lambda x: x[2]).next()[1] if len(keys) > 0 else ()
        keys = map(lambda x: {'Column_name': x[4]}, group)
        logger.debug(keys)
        for key in keys:
            key_field = filter(lambda x: x['Field'] == key['Column_name'], fields)[0]
            field_list[key_field['Field']] = self.load_field_info(key_field, is_key=True)

        for field in fields:
            if not field_list.has_key(field['Field']):
                field_list[field['Field']] = self.load_field_info(field)
        return field_list.values()

    def load_metadata(self, tablename, auto_fill=True):
        return self.load_table_metadata(tablename)


    def load_field_info(self, field_info, is_key=False):
        field_datatype = field_info["Type"].split('(')[0]
        field_length = 0
        field_precision = 0
        if field_datatype == "bigint":
            return IntField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'datetime':
            return DatetimeField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'varchar':
            return StringField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'smallint':
            return IntField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'int':
            return IntField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'longtext':
            return StringField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'date':
            return DateField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'decimal':
            return DecimalField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'text':
            return StringField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'char':
            return StringField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'bit':
            return IntField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'tinyint':
            return IntField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'longblob':
            return BinaryField(field_info['Field'], is_key=is_key)
        elif field_datatype == 'binary':
            return BinaryField(field_info['Field'], is_key=is_key)
        else:
            raise Exception('Unsupportted type ' + field_datatype)

    def set_metadata(self, tablename, fields):
        field_dict = collections.OrderedDict()
        for field in fields:
            field_dict[field.name] = field
        self._metadata[tablename] = field_dict
        self._realtablename[tablename] = tablename
        return field_dict
        
    def _generate_insert_value(self, field, value):
        if isinstance(field, StringField):
            return '\'' + value.replace('\'', '\'\'') + '\'' 
        if isinstance(field, DatetimeField):
            return '\'' + value + '\''
        return str(value)

    def close(self):
        self.cnx.close()
         
        

class MySQLDialect(Dialect):
    pass
    