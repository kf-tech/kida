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

class Meta:
    def __init__(self):
        self.__tables = []

    def add_table(self, table):
        if not isinstance(table, Table):
            raise Exception('Wrong table type.')

        logger.debug(self.__tables)
        for existing_table in self.__tables:
            if existing_table.tablename.lower() == table.tablename.lower():
                raise Exception('Table already exists')

        self.__tables.append(table)

    # def get_table(self, table_name):
    #     table_name = table_name.lower()
    #     for table in self.__tables:
    #         if table.tablename.lower() == table_name:
    #             return table

    def __getitem__(self, table_name):
        for table in self.__tables:
            if table.tablename.lower() == table_name:
                return table


class Row:
    def __init__(self, table, data_dict):
        self.values = {}
        for key, value in data_dict.items():
            field = table.get_field(key)
            if field:
                self.values[field] = value



