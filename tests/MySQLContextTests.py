import unittest

#from PyDB.MySQLContext import MySQLContext

import PyDB
import logging
import time



class Test(unittest.TestCase):

    
    def setUp(self):
        self.target = PyDB.MySQLContext({'user':'root', 'host':'localhost', 'db':'test'})
        logging.basicConfig(level=logging.DEBUG)
        """
CREATE TABLE `table1` (
  `id` int(11) NOT NULL,
  `fint` int(11) DEFAULT NULL,
  `fstr` varchar(50) DEFAULT NULL,
  `flong` bigint(20) DEFAULT NULL,
  `fdate` date DEFAULT NULL,
  `fdatetime` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """
        
        """
            create table table2 (
                k1 int not null,
                k2 int not null,
                primary key (k2, k1))
        """

    def tearDown(self):
        pass


    def testName(self):
        pass
    
    def test_save(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, [
                                       PyDB.IntField("id", is_key=True),
                                       PyDB.StringField("fstr"),
                                       PyDB.IntField("fint")
                                       ])
        
        data = {"id" : 1, "fint": 1, "fstr": 'abc'}
        context.save(tablename, data)
        context.commit()
    
    def test_build_context(self):
        context = self.target
        self.assertIsNotNone(context, "context is none")
        self.assertIsInstance(context, PyDB.MySQLContext, "context is not MySQLContext")
        
    def test_metadata(self):
        context = self.target
        context.set_metadata("table1", [
                                        PyDB.IntField("id", is_key = True)
                                        ])
        data = {"id": 1, "fielda" : 'a'}
        tablename = 'table1'
        context.save(tablename, data)
        
    def test_load_metadata(self):
        context = self.target
        fields = context.load_metadata('ann_announcementinfo')
        for field in fields:
            print field
            
    def test_load_metadata2(self):
        context = self.target
        fields = context.load_metadata('table1')
        fields = context.set_metadata('table1', fields)
        for field in fields.values():
            print field
        
        self.assertTrue(fields['id'].is_key)
            
    def test_save2(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        data = {"fint": 1, "fstr": 'ab\'c'}
        context.save(tablename, data)
        context.commit()
        
    def test_product_database_types(self):
        context = self.target
        cursor = context.execute_sql("show tables")
        rows = cursor.fetchall()
#         for row in cursor:
#             rows.append(row)
        
        for row in rows:
            context.load_metadata(row[0])
            print row
            
    def test_save_or_update(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        data = {
                "id" : 1, 
                "fint": 2, 
                "fstr": 'abc', 
                'fdate': time.time(), 
                'fdatetime': time.time()}
        context.save_or_update(tablename, data)
        context.commit()
    
    def test_get_1(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        id = 1
        rows = context.get(tablename, {'id' : id})
        self.assertEqual(1, len(rows), "ResultSet's size is not 1")
        self.assertEqual(id, rows[0]['id'], 'Result rows id is not %s' % id)
        
    def test_save_10000(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        for i in xrange(10000):
            data = {'id': i, "fint": 1, "fstr": 'ab\'c'}
            context.save(tablename, data)
            context.commit()
    
    def test_index_sequence(self):
        # since the primary keys order might not match the on of fields, it 
        # should as the sequence of indexes 
        context = self.target
        tablename = 'table2'

        context.set_metadata(tablename, context.load_metadata(tablename))
        list = context._metadata[tablename].values()
        self.assertEqual("k2", list[0].name, "The first field should be k2")
        self.assertEqual("k1", list[1].name, "The second field should be k1")
        
    def test_index_sequence_save(self):
        # since the primary keys order might not match the on of fields, it 
        # should as the sequence of indexes 
        context = self.target
        tablename = 'table2'
        
        data = {'k1' : 1, 'k2' : 2}

        context.set_metadata(tablename, context.load_metadata(tablename))
        context.save(tablename, data)
        context.commit()
        
    def test_exists_key(self):
        context = self.target
        tablename = 'table2'
        keys = {'k1' : 1, 'k2' : 2}

        context.set_metadata(tablename, context.load_metadata(tablename))
        self.assertTrue(context.exists_key(tablename, keys)) 
    
    def test_not_existing_table_metadata(self):
        context = self.target
        tablename = 'table_not_existing'
        self.assertIsNone(context.load_metadata(tablename), "Metadata should be none")

if __name__ == "__main__":
    import sys;sys.argv = ['', 'Test.test_not_existing_table_metadata']
    unittest.main()