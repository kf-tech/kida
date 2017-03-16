

class Table:
    __tablename = None
    __fields = []

    def __init__(self, tablename, fields):
        self.__tablename = tablename
        self.__fields = fields

    @property
    def tablename(self):
        return self.__tablename



class Meta:
    __tables = []

    def add_table(self, table):
        if not isinstance(table, Table):
            raise Exception('Wrong table type.')

        for existing_table in self.__tables:
            if existing_table.tablename.lower() == table.tablename.lower():
                raise Exception('Table already exists')

        self.__tables.append(table)

    def get_table(self, table_name):
        table_name = table_name.lower()
        for table in self.__tables:
            if table.tablename.lower() == table_name:
                return table
