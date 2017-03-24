'''
'''
from kida import DbContext
from kida.fields import IntField, StringField, DatetimeField, DateField, DecimalField, BinaryField
from DbContext import Dialect, KEY_TYPE_PRIMARY, KEY_TYPE_UNIQUE_INDEX, KEY_TYPE_UNIQUE_KEY
import logging
import collections
from .exceptions import *
from itertools import groupby
import urlparse
from .common import Table, Row

logger = logging.getLogger(__name__)

class MySQLContext(DbContext):
    '''
    classdocs
    '''

    def __init__(self, dburl=None, meta=None, user=None, passwd=None, *args, **kwargs):
        '''
        Constructor
        '''
        import MySQLdb
        super(MySQLContext, self).__init__(meta)

        if dburl is not None and isinstance(dburl, dict):
            params = dburl.copy()
            params.update(kwargs)
            if user is not None:
                params.update(user=user)
            if passwd is not None:
                params.update(passwd = passwd)
            self.cnx = MySQLdb.connect(**params)
        elif dburl is not None:
            urlparts = urlparse.urlparse(dburl)
            username = urlparts.username or user
            password = urlparts.password or passwd
            host = urlparts.hostname
            port = int(urlparts.port) if urlparts.port is not None else 3306
            dbname = urlparts.path.lstrip('/')
            params = {}
            for key , values in urlparse.parse_qs(urlparts.query).items():
                params.update({key: values[0]})
            if username is not None:
                params.update(user=username)
            if password is not None:
                params.update(passwd=password)
            params.update(host=host, port=port, db=dbname)
            logger.debug(params)
            self.cnx = MySQLdb.connect(*args, **params)
        else:
            params = kwargs.copy()
            if user is not None:
                params.update(user=user)
            if passwd is not None:
                params.update(passwd=passwd)
            self.cnx = MySQLdb.connect(*args, **params)

        self.dialect = MySQLDialect()
    
    def execute_sql(self, sql, params=None, dict_cursor=False):
        import MySQLdb
        from _mysql_exceptions import OperationalError
        logger.debug(sql)
        try:
            if dict_cursor:
                cursor = self.cnx.cursor(MySQLdb.cursors.DictCursor)
            else:
                cursor = self.cnx.cursor()
            cursor.execute(sql, params)
            return cursor
        except OperationalError:
            raise

    def _save(self, tablename, data):
        table = self._meta[tablename]
        row = Row(table, data)
        fields = ','.join([field.name for field in row.values.keys()])
        values = ','.join(['%({0})s'.format(field.name) for field in row.values.keys()])
        data = {field.name : value for field, value in row.values.items()}
        sql = 'insert into ' + table.tablename + ' (' + fields + ') values (' + values + ')'
        logger.debug(sql)
        return self.execute_sql(sql, data)

    def save(self, tablename, data):
        self.save_or_update(tablename, data)
    
    def update(self, tablename, data):
        table = self._meta[tablename]
        row = Row(table, data)

        key_fields = [field for field in table.fields if field.is_key]

        sql = """update """ + table.tablename + " set "
        
        sql += ','.join(['{0}=%({0})s'.format(field.name) for field in row.values.keys()])
        key_condition = ' and'.join([' {0}=%({0})s '.format(field.name) for field in key_fields])
        sql += " where " + key_condition
        logger.debug(sql)
        return self.execute_sql(sql, data)

    def save_batch(self, tablename, rows):
        self.cnx.autocommit(False)
        for row in rows:
            if self.get(tablename, row):
                self.update(tablename, row)
            else:
                self._save(tablename, row)

        self.cnx.commit()
        self.cnx.autocommit(True)
    
    def save_or_update(self, tablename, data):
        table = self._meta[tablename]
        key_fields = []
        for field in table.fields:
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
        table = self._meta[tablename]
        sql_field = ','.join([field.name for field in table.fields])
        sql += sql_field + ' from ' + table.tablename
        key_fields = [field for field in table.fields if field.is_key]
        
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
        table = self._meta[tablename]
        sql += ' from `' + table.tablename + '`'
        key_fields = [field for field in table.fields if field.is_key]
        
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

    def load_table_metadata(self, tablename, auto_fill=False, key_type=KEY_TYPE_PRIMARY):
        is_primary = True
        sql = 'show tables'
        cursor = self.execute_sql(sql)
        tables = cursor.fetchall()
        tables = {x[0].upper(): x[0] for x in tables}
        if not tables.has_key(tablename.upper()):
            logger.error("Table '%s' doesn't exist" % tablename)
            return None

        sql = 'show columns from ' + tablename
        cursor = self.execute_sql(sql)

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
        if key_type == KEY_TYPE_PRIMARY:
            sql = 'show index from ' + tablename + " where Non_unique = 0 and key_name='PRIMARY'"
        elif key_type == KEY_TYPE_UNIQUE_KEY or key_type == KEY_TYPE_UNIQUE_INDEX:
            sql = 'show index from ' + tablename + " where Non_unique = 0 and key_name<>'PRIMARY'"
        else:
            raise Exception('Not supported key_type %s' % key_type)
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

    def load_metadata(self, tablename, key_type=KEY_TYPE_PRIMARY):
        return self.load_table_metadata(tablename, key_type=key_type)


    def load_field_info(self, field_info, is_key=False):
        field_datatype = field_info["Type"].split('(')[0]
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
        self._meta.add_table(Table(tablename, fields))
        return field_dict

    def close(self):
        self.cnx.close()


class MySQLDialect(Dialect):
    pass
    