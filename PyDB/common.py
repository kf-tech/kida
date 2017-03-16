

class TableDefinition:
    __tablename = None
    __fields = []

    @property
    def tablename(self):
        return self.__tablename

