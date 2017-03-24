import logging

logger = logging.getLogger(__name__)


class Table:

    def __init__(self, tablename, fields):
        self.__tablename = tablename
        self.__fields = fields

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
        raise KeyError('table not exists')


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






