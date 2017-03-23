import unittest
import PyDB
import logging
from PyDB.exceptions import *
import datetime

def setup_module():
    context = PyDB.MySQLContext(user='root', host='localhost')
    context.execute_sql('create database pydb_test')
    context.execute_sql('use pydb_test')
    context.execute_sql("create user 'pydb_test'@'localhost' IDENTIFIED BY 'password'")
    context.execute_sql("GRANT ALL ON pydb_test.* TO 'pydb_test'@'localhost';")

    context.execute_sql(        """
CREATE TABLE `table1` (
  `id` int(11) NOT NULL auto_increment,
  `fint` int(11) DEFAULT NULL,
  `fstr` varchar(50) DEFAULT NULL,
  `flong` bigint(20) DEFAULT NULL,
  `fdate` date DEFAULT NULL,
  `fdatetime` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """)

    context.execute_sql(        """
            create table table2 (
                k1 int not null,
                k2 int not null,
                primary key (k2, k1))
        """)

    context.execute_sql('''
            create table users (
                id int not null auto_increment primary key,
                username varchar(20) not null,
                constraint unique idx_users_username (username)
            )
        ''')

    context.close()
    logging.debug('setup module')

def teardown_module():
    context = PyDB.MySQLContext({'user':'root', 'host':'localhost', 'db':'pydb_test'})
    context.execute_sql('drop database pydb_test')
    context.execute_sql("drop user pydb_test@'localhost'")
    context.close()
    logging.debug('teardown module')


class Test(unittest.TestCase):
    def setUp(self):
        self.target = PyDB.MySQLContext({'user':'pydb_test', 'host':'localhost', 'passwd': 'password', 'db':'pydb_test'})
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
  PRIMARY KEY (`id`),
) ENGINE=InnoDB DEFAULT CHARSET=latin1
        """

        """
            create table table2 (
                k1 int not null,
                k2 int not null,
                primary key (k2, k1))
        """

        '''
            create table users (
                id int not null auto_increment primary key,
                username varchar(20) not null,
                constraint unique idx_users_username (username)
            )
        '''

    def tearDown(self):
        logging.debug('tearDown')
        self.target.close()
        self.target = None

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

    def test_save_tablename_insentisive(self):
        context = self.target
        tablename = 'table1'
        tablename_upper = 'TABLE1'
        context.set_metadata(tablename, [
            PyDB.IntField("id", is_key=True),
            PyDB.StringField("fstr"),
            PyDB.IntField("fint")
        ])

        data = {"id": 1, "fint": 1, "fstr": 'abc'}
        context.save(tablename_upper, data)
        context.commit()

        row = context.get(tablename_upper, {'id': 1})
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

    def test_set_metadata(self):
        context = self.target
        context.set_metadata("table1", [
                                        PyDB.IntField("id", is_key = True)
                                        ])
        data = {"id": 1, "fielda" : 'a'}
        tablename = 'table1'
        context.save(tablename, data)
        
    def test_load_metadata_default_primarykey(self):
        context = self.target
        fields = context.load_metadata('table1')
        self.assertEqual(len(fields), 6)

        self.assertEqual('id', fields[0].name)
        self.assertTrue(fields[0].is_key)
        self.assertEqual(type(fields[0]), PyDB.IntField)

        self.assertEqual('fint', fields[1].name)
        self.assertFalse(fields[1].is_key)
        self.assertEqual(type(fields[1]), PyDB.IntField)

        self.assertEqual('fstr', fields[2].name)
        self.assertFalse(fields[2].is_key)
        self.assertEqual(type(fields[2]), PyDB.StringField)

        self.assertEqual('flong', fields[3].name)
        self.assertFalse(fields[3].is_key)
        self.assertEqual(type(fields[3]), PyDB.IntField)

        self.assertEqual('fdate', fields[4].name)
        self.assertFalse(fields[4].is_key)
        self.assertEqual(type(fields[4]), PyDB.DateField)

        self.assertEqual('fdatetime', fields[5].name)
        self.assertFalse(fields[5].is_key)
        self.assertEqual(type(fields[5]), PyDB.DatetimeField)

    def test_load_metadata_default_uniquekey(self):
        context = self.target
        fields = context.load_metadata('users', key_type=PyDB.KEY_TYPE_UNIQUE_KEY)
        self.assertEqual(len(fields), 2)

        # keys go first
        self.assertEqual('username', fields[0].name)
        self.assertTrue(fields[0].is_key)
        self.assertEqual(type(fields[0]), PyDB.StringField)

        self.assertEqual('id', fields[1].name)
        self.assertFalse(fields[1].is_key)
        self.assertEqual(type(fields[1]), PyDB.IntField)

            
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
        list = context._meta[tablename].fields
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
        dburl = 'mysql://pydb_test:password@localhost/pydb_test'
        db_context = PyDB.MySQLContext(dburl)
        db_context.close()

    def test_connect_by_url_plus_user(self):
        dburl = 'mysql://localhost/pydb_test'
        db_context = PyDB.MySQLContext(dburl, user='pydb_test', passwd='password')
        db_context.close()

    def test_connect_by_url_no_username(self):
        dburl = 'mysql://root@localhost/pydb_test'
        db_context = PyDB.MySQLContext(dburl)
        db_context.close()

    def test_connect_by_dict(self):
        params = {'user': 'pydb_test', 'passwd':'password', 'host': 'localhost', 'db': 'pydb_test'}
        db_context = PyDB.MySQLContext(params)
        db_context.close()

    def test_connect_by_kwargs(self):
        db_context = PyDB.MySQLContext(user='root', host='localhost', port=3306, db='pydb_test')
        db_context.close()

