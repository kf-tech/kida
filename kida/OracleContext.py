'''
'''
from kida import DbContext
from fields import IntField, StringField, DatetimeField, DateField, DecimalField, BinaryField
from DbContext import Dialect, KEY_TYPE_PRIMARY, KEY_TYPE_UNIQUE_INDEX, KEY_TYPE_UNIQUE_KEY
import logging
import datetime
import urlparse
import os
from .common import Row, Table, Meta
from .exceptions import *
os.environ["NLS_LANG"] = ".UTF8"

logger = logging.getLogger('PyDB')

class CaseInsensitiveDict(dict):
    @classmethod
    def _k(cls, key):
        return key.lower() if isinstance(key, basestring) else key

    def __init__(self, *args, **kwargs):
        super(CaseInsensitiveDict, self).__init__(*args, **kwargs)
        self._convert_keys()
    def __getitem__(self, key):
        return super(CaseInsensitiveDict, self).__getitem__(self.__class__._k(key))
    def __setitem__(self, key, value):
        super(CaseInsensitiveDict, self).__setitem__(self.__class__._k(key), value)
    def __delitem__(self, key):
        return super(CaseInsensitiveDict, self).__delitem__(self.__class__._k(key))
    def __contains__(self, key):
        return super(CaseInsensitiveDict, self).__contains__(self.__class__._k(key))
    def has_key(self, key):
        return super(CaseInsensitiveDict, self).has_key(self.__class__._k(key))
    def pop(self, key, *args, **kwargs):
        return super(CaseInsensitiveDict, self).pop(self.__class__._k(key), *args, **kwargs)
    def get(self, key, *args, **kwargs):
        return super(CaseInsensitiveDict, self).get(self.__class__._k(key), *args, **kwargs)
    def setdefault(self, key, *args, **kwargs):
        return super(CaseInsensitiveDict, self).setdefault(self.__class__._k(key), *args, **kwargs)
    def update(self, E={}, **F):
        super(CaseInsensitiveDict, self).update(self.__class__(E))
        super(CaseInsensitiveDict, self).update(self.__class__(**F))
    def _convert_keys(self):
        for k in list(self.keys()):
            v = super(CaseInsensitiveDict, self).pop(k)
            self.__setitem__(k, v)



class OracleContext(DbContext):
    '''
    classdocs
    '''
    

    def __init__(self, dburl=None, meta=None, user=None, password=None, host=None, port=None, sid=None, service_name=None, **kwargs):
        '''
        Constructor
        '''
        super(OracleContext, self).__init__(meta)

        params = {}
        if dburl is not None:
            urlparts = urlparse.urlparse(dburl)
            if urlparts.username:
                params['user'] = urlparts.username
            if urlparts.password:
                params['password'] = urlparts.password

            port_inurl = None

            if urlparts.port:
            #    params['port'] = int(urlparts.port)
                port_inurl = int(urlparts.port)
            port = port or port_inurl or 1521
            #if urlparts.hostname:
            #    params['host'] = urlparts.hostname
            host = host or urlparts.hostname
            service_name = service_name or urlparts.path.lstrip('/')
            for key, values in urlparse.parse_qs(urlparts.query):
                params[key] = values[0]

        import cx_Oracle
        params.update(kwargs)
        if user:
            params.update(user = user)
        if password:
            params.update(password=password)
        self._metadata = {}
        if sid:
            dsn = cx_Oracle.makedsn(host, port, sid=sid)
        else:
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        logger.debug('dsn: %s' % dsn)
        logger.debug('host: %s' % host)
        self._context = cx_Oracle.connect(dsn=dsn, **params)
        self._cursor = self._context.cursor()
        self._cursor.execute("ALTER SESSION  SET NLS_DATE_FORMAT='YYYY-MM-DD'")
        
    def execute_sql(self, sql, params=None):
        cursor = self._cursor
        params = params or {}
        cursor.execute(sql, params)
        logger.debug(sql)
        if params:
            logger.debug(params)
        return cursor

    def _save(self, tablename, data):
        table = self._meta[tablename]
        row = Row(table, data)
        fields = ','.join([field.name for field in row.values.keys()])
        values = ','.join([':{0}'.format(field.name) for field in row.values.keys()])
        data = {field.name : value for field, value in row.values.items()}
        sql = 'insert into ' + table.tablename + ' (' + fields + ') values (' + values + ')'
        logger.debug(sql)
        return self.execute_sql(sql, data)
    
    def save(self, tablename, data):
        self.save_or_update(tablename, data)
    
    def load_metadata(self, tablename, auto_fill=False, key_type=KEY_TYPE_PRIMARY):
        tablename = tablename.upper()
        if key_type == KEY_TYPE_PRIMARY:
            sql = """
            select user_tab_cols.COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                NULLABLE, CONSTRAINT_TYPE from user_tab_cols
            left join(
                select user_cons_columns.TABLE_NAME, user_cons_columns.COLUMN_NAME,
                    user_constraints.CONSTRAINT_TYPE,user_cons_columns.POSITION
                from user_cons_columns
                left join user_constraints
                    on user_constraints.CONSTRAINT_TYPE = 'P'
                    and user_constraints.TABLE_NAME = user_cons_columns.TABLE_NAME
                    and user_constraints.CONSTRAINT_NAME = user_cons_columns.CONSTRAINT_NAME
                where user_cons_columns.TABLE_NAME = '%(TableName)s'
                ) key_columns
                on user_tab_cols.TABLE_NAME = key_columns.TABLE_NAME
                and user_tab_cols.COLUMN_NAME = key_columns.COLUMN_NAME
                and key_columns.CONSTRAINT_TYPE = 'P'
            where user_tab_cols.TABLE_NAME = '%(TableName)s'
            """
            sql = sql % {"TableName" : tablename}
        elif key_type == KEY_TYPE_UNIQUE_KEY:
            sql = """
                select user_tab_cols.COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION,
                NULLABLE, CONSTRAINT_TYPE from user_tab_cols
            left join(
                select user_cons_columns.TABLE_NAME, user_cons_columns.COLUMN_NAME,
                    user_constraints.CONSTRAINT_TYPE,user_cons_columns.POSITION
                from user_cons_columns
                left join (select * from user_constraints where CONSTRAINT_TYPE='U' and rownum=1)
                    user_constraints
                    on user_constraints.CONSTRAINT_TYPE = 'U'
                    and user_constraints.TABLE_NAME = user_cons_columns.TABLE_NAME
                    and user_constraints.CONSTRAINT_NAME = user_cons_columns.CONSTRAINT_NAME
                where user_cons_columns.TABLE_NAME = '%(TableName)s'
                ) key_columns
                on user_tab_cols.TABLE_NAME = key_columns.TABLE_NAME
                and user_tab_cols.COLUMN_NAME = key_columns.COLUMN_NAME
                and key_columns.CONSTRAINT_TYPE = 'U'
            where user_tab_cols.TABLE_NAME = '%(TableName)s'
            """
            sql = sql % {"TableName": tablename}
        else:
            raise Exception('Key type not supported %s '% key_type)
        cursor = self.execute_sql(sql, None)
        fields = []
        for row in cursor:
            field_info = {
                     'Field' : row[0],
                     'Type' : row[1],
                     'Length' : row[2],
                     'Precision' : row[3],
                     'Nullable' : row[4],
                     'Key' : row[5]
                     }
            fields.append(self.load_field_info(field_info))
        if len(fields)==0:
            raise TableNotExistError()
        return fields

    def load_field_info(self, field_info):
        field_datatype = field_info["Type"]
        is_key = field_info['Key'] in ('P', 'U')
        field_name = field_info['Field']
        if field_datatype == "NUMBER" and (field_info['Precision'] == 0 or field_info['Precision'] == None):
            return IntField(field_name, is_key=is_key)
        elif field_datatype == 'NUMBER' and field_info['Precision'] > 0:
            return DecimalField(field_name, is_key=is_key )
        elif field_datatype == 'NVARCHAR2':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'VARCHAR2':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'FLOAT':
            return DecimalField(field_name, is_key=is_key)
        elif field_datatype == 'CHAR':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'TIMESTAMP(6)':
            return DatetimeField(field_name, is_key=is_key)
        elif field_datatype == 'CLOB':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'DATE':
            return DatetimeField(field_name, is_key=is_key)
        else:
            raise Exception('Unsupportted type ' + field_datatype)
                
    def set_metadata(self, tablename, fields):
        field_dict = {}
        for field in fields:
            field_dict[field.name] = field
        self._metadata[tablename] = field_dict
        self._meta.add_table(Table(tablename, fields))
        return field_dict
        
    def save_or_update(self, tablename, data):
        table = self._meta[tablename]
        key_fields = [field for field in table.fields if field.is_key]
        key_signed = False  # find whether key field is specified in data
        row = Row(table, data)
        for key_field in key_fields:
            if row[key_field.name]:
                key_signed = True

        logger.debug(data)
        if key_signed:
            if self.exists_key(tablename, data):
                return self.update(tablename, data)
        else:
            raise TableKeyNotSpecified()

        return self._save(tablename, data)

    def _rows_as_dicts(self, cursor):
        """ returns cx_Oracle rows as dicts """
        colnames = [i[0] for i in cursor.description]
        for row in cursor:
            yield dict(zip(colnames, row))
    
    def get(self, tablename, keys):
        sql = 'select '
        table = self._meta[tablename]
        sql_field = ','.join([field.name for field in table.fields])
        sql += sql_field + ' from ' + tablename
        key_fields = [field for field in table.fields if field.is_key]

        key_condition = ' and '.join([' %s = :%s ' % (key.name, key.name) for key in key_fields])
        sql += ' where ' + key_condition

        cursor = self.execute_sql(sql, keys)
        results = list(self._rows_as_dicts(cursor))
        if len(results) == 0:
            return None
        if len(results) ==1:
            return Row(table, results[0])
        else:
            raise Exception("More than one rows with the key fetched")
    
    def exists_key(self, tablename, keys):
        sql = 'select count(*)'
        table = self._meta[tablename]
        sql += ' from ' + tablename + ''
        key_fields = [field for field in table.fields if field.is_key]
        
        key_condition = 'and'.join([' %s = :%s ' % (key.name, key.name) for key in key_fields])
        sql += ' where ' + key_condition

        keys = CaseInsensitiveDict(keys)
        
        params = dict([(key.name, keys[key.name]) for key in key_fields])
        cursor = self.execute_sql(sql, params)
        ret = cursor.fetchone()
        if ret[0] > 0:
            return True
        return False
    
    def update(self, tablename, data):
        table = self._meta[tablename]
        key_fields = [field for field in table.fields if field.is_key]
        sql = """update """ + tablename + " set "
        row = Row(table, data)
        sql += ','.join(['%s=:%s' % (field.name,field.name) for field in row.values.keys()])

        key_condition = 'and'.join([' %s = :%s ' % (key.name, key.name) for key in key_fields])

        params = {field.name: value for field, value in row.values.items()}
        sql += " where " + key_condition
        return self.execute_sql(sql, params)
        
    def commit(self):
        self._context.commit()

    def close(self):
        self._context.close()
