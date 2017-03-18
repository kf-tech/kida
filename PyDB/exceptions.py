

class TableNotExistError(Exception):
    def __init__(self):
        super(TableNotExistError,self).__init__('Table dost not exist.')

class TableKeyNotSpecified(Exception):
    def __init__(self):
        super(TableKeyNotSpecified, self).__init__('Table key is not specified.')