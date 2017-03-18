'''

'''
import unittest
import PyDB
import logging
import cx_Oracle
import time

@unittest.skip
class Test(unittest.TestCase):

    def setUp(self):
        self.target = PyDB.OracleContext("sales/sales@192.168.101.149/orcl")
        logging.basicConfig(level=logging.DEBUG)
        
        sql_create_table1 = """
        create table TABLE1 (
        id int not null primary key,
        fint int not null,
        fstr nvarchar2(20) not null
        );
        """

    def tearDown(self):
        pass

    def testName(self):
        pass
    
    def test_load_metadata(self):
        context = self.target
        fields = context.load_metadata("TABLE1")
        
        for field in fields:
            print field
            
    def test_save(self):
        context = self.target
        tablename = 'TABLE1'
        context.set_metadata(tablename, [
                                       PyDB.IntField("ID", is_key=True),
                                       PyDB.StringField("FSTR"),
                                       PyDB.IntField("FINT")
                                       ])
        
        data = {"ID" : 1, "FINT": 1, "FSTR": 'abc'}
        context.save(tablename, data)
        context.commit()
        
    def test_save_or_update(self):
        context = self.target
        tablename = 'TABLE1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        data = {
                "ID" : 1, 
                "FINT": 2, 
                "FSTR": 'abc\'d', 
                'FDATE': time.time(), 
                'FDATETIME': time.time()}
        context.save_or_update(tablename, data)
        context.commit()


if __name__ == "__main__":
    import sys;sys.argv = ['', 'Test.test_save_or_update']
    unittest.main()