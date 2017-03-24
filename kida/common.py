import logging
from .exceptions import *

logger = logging.getLogger(__name__)

class ColumnCollection:
    def __init__(self, *columns):
        self._all_columns = []
        for c in columns:
            self._all_columns.append(c)

    def __getitem__(self, item):
        if isinstance(item, int):
            return self._all_columns[item]
        elif isinstance(item, basestring):
            for c in self._all_columns:
                if c.name == item:
                    return c

    def __contains__(self, item):
        if not isinstance(item, basestring):
            raise Exception('__contains__ requires a string argument')
        for c in self._all_columns:
            if c.name == item:
                return True

        return False

    def __len__(self):
        return len(self._all_columns)

class TableCollection:
    def __init__(self, tables):
        self._tables = tables

    def __contains__(self, item):
        if not isinstance(item, basestring):
            raise Exception('__contains__ requires a string argument')
        for c in self._tables:
            if c.name == item:
                return True

        return False

class Table:

    def __init__(self, tablename, fields):
        self.__tablename = tablename
        self.__fields = fields
        self._columns = ColumnCollection(*fields)

    @property
    def tablename(self):
        return self.__tablename

    def __repr__(self):
        return 'Table %s' % self.__tablename

    def get_field(self, field_name):
        for field in self.__fields:
            if field.name.lower() == field_name.lower():
                return field

    @property
    def fields(self):
        return self.__fields


    @property
    def columns(self):
        return self._columns

class Meta:
    def __init__(self):
        self.__tables = []

    def add_table(self, table):
        if not isinstance(table, Table):
            raise Exception('Wrong table type.')

        for existing_table in self.__tables:
            if existing_table.tablename.lower() == table.tablename.lower():
                raise Exception('Table already exists')

        self.__tables.append(table)

    def __getitem__(self, table_name):
        for table in self.__tables:
            if table.tablename.lower() == table_name.lower():
                return table
        raise TableNotExistError()

    @property
    def tables(self):
        return TableCollection(self.__tables)


class Row(object):
    def __init__(self, table, data_dict):
        self.values = {}
        for key, value in data_dict.items():
            field = table.get_field(key)
            if field:
                self.values[field] = value

    def __getitem__(self, item):
        for key in self.values.keys():
            if key.name.lower() == item.lower():
                return self.values[key]\

    def __len__(self):
        return self.values.__len__()






