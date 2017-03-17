import unittest

#from PyDB.MySQLContext import MySQLContext

import PyDB
import logging
import time
from PyDB.exceptions import *
import datetime



class Test(unittest.TestCase):

    def setUp(self):
        self.target = PyDB.MySQLContext({'user':'root', 'host':'localhost', 'db':'test'})
        logging.debug('setUp')
        self.target.execute_sql('delete from table1')
        self.target.execute_sql('delete from table2')
        """
CREATE TABLE `table1` (
  `id` int(11) NOT NULL auto_increment,
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
        logging.debug('tearDown')
        self.target.close()

    def test_build_context(self):
        context = self.target
        self.assertIsNotNone(context, "context is none")
        self.assertIsInstance(context, PyDB.MySQLContext, "context is not MySQLContext")

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

        row = context.get(tablename, {'id':1})
        logging.debug(row)
        self.assertIsNotNone(row)
        self.assertEqual(row['fstr'], 'abc')
        self.assertEqual(row['fint'], 1)

    def test_save_update(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, [
            PyDB.IntField("id", is_key=True),
            PyDB.StringField("fstr"),
            PyDB.IntField("fint")
        ])

        data = {"id": 1, "fint": 1, "fstr": 'abc'}
        context.save(tablename, data)

        data['fint'] = 2
        data['fstr'] = 'abcd'
        context.save(tablename, data)

        row = context.get(tablename, {'id': 1})
        logging.debug(row)
        self.assertIsNotNone(row)
        self.assertEqual(row['fstr'], 'abcd')
        self.assertEqual(row['fint'], 2)

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
        fields = context.load_metadata('table1')
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
        try:
            context.save(tablename, data)
            self.fail('Did not catch the exception.')
        except TableKeyNotSpecified:
            pass

        
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
                'fdate': datetime.date.today(),
                'fdatetime': datetime.datetime.now()}
        context.save_or_update(tablename, data)
        context.commit()
    
    def test_get_1(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        id = 1
        data = {'id':id}
        context.save(tablename, data)
        row = context.get(tablename, {'id' : id})
        logging.debug(row)
        #self.assertEqual(1, len(rows), "ResultSet's size is not 1")
        self.assertEqual(id, row['id'], 'Result rows id is not %s' % id)

    @unittest.skip
    def test_save_10000(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        for i in xrange(10000):
            data = {'id': i, "fint": 1, "fstr": 'ab\'c'}
            context.save(tablename, data)
            context.commit()

    def test_save_10000_batch(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, context.load_metadata(tablename))
        rows = []
        for i in xrange(10000):
            data = {'id': i, "fint": 1, "fstr": 'ab\'c'}
            rows.append(data)
        context.save_batch(tablename, rows)

    
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
        context.save(tablename, keys)
        self.assertTrue(context.exists_key(tablename, keys)) 
    
    def test_not_existing_table_metadata(self):
        context = self.target
        tablename = 'table_not_existing'
        self.assertIsNone(context.load_metadata(tablename), "Metadata should be none")


class DBConnectionTest(unittest.TestCase):
    def test_connect_by_url(self):
        dburl = 'mysql://root@localhost/test'
        db_context = PyDB.MySQLContext(dburl)
        db_context.close()

    def test_connect_by_url_no_username(self):
        dburl = 'mysql://localhost/test'
        db_context = PyDB.MySQLContext(dburl)
        db_context.close()

    def test_connect_by_dict(self):
        params = {'user': 'root', 'host': 'localhost', 'db': 'test'}
        db_context = PyDB.MySQLContext(params)
        db_context.close()

    def test_connect_by_kwargs(self):
        db_context = PyDB.MySQLContext(user='root', host='localhost', port=3306, db='test')
        db_context.close()

