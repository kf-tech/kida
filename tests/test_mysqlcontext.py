import unittest
import kida
import logging
from kida.exceptions import *
import datetime
from nose.tools import *

db_host = 'localhost'
manager_username = 'root'
manager_password = ''
test_username = 'pydb_test'
test_password = 'password'
test_db = 'pydb_test'
manager_dburl = 'mysql://root@localhost'
target_dburl = 'mysql://%s:%s@%s/%s' % (test_username, test_password, db_host, test_db)

def setup_module():
    context = kida.MySQLContext(user=manager_username, host=db_host)
    context.execute_sql('create database %s' % test_db)
    context.execute_sql('use %s' % test_db)
    context.execute_sql("create user '%s'@'localhost' IDENTIFIED BY '%s'" % (test_username, test_password))
    context.execute_sql("GRANT ALL ON %s.* TO '%s'@'localhost';" % (test_db, test_username))

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
    context = kida.MySQLContext({'user': 'root', 'host': 'localhost', 'db': 'pydb_test'})
    context.execute_sql('drop database pydb_test')
    context.execute_sql("drop user pydb_test@'localhost'")
    context.close()
    logging.debug('teardown module')


class Test(unittest.TestCase):
    def setUp(self):
        self.target = kida.MySQLContext(dburl=target_dburl)
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
                                       kida.IntField("id", is_key=True),
                                       kida.StringField("fstr"),
                                       kida.IntField("fint")
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
            kida.IntField("id", is_key=True),
            kida.StringField("fstr"),
            kida.IntField("fint")
        ])

        data = {"id": 1, "fint": 1, "fstr": 'abc'}
        context.save(tablename_upper, data)
        context.commit()

        row = context.get(tablename_upper, {'id': 1})
        logging.debug(row);
        self.assertIsNotNone(row)
        self.assertEqual(row['fstr'], 'abc')
        self.assertEqual(row['fint'], 1)

    def test_save_update(self):
        context = self.target
        tablename = 'table1'
        context.set_metadata(tablename, [
            kida.IntField("id", is_key=True),
            kida.StringField("fstr"),
            kida.IntField("fint")
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
                                        kida.IntField("id", is_key = True)
                                        ])
        data = {"id": 1, "fielda" : 'a'}
        tablename = 'table1'
        context.save(tablename, data)
        
    def test_load_metadata_default_primarykey(self):
        context = self.target
        table = context.load_metadata('table1')
        columns = table.columns
        self.assertEqual(len(columns), 6)

        self.assertEqual('id', columns[0].name)
        self.assertTrue(columns[0].is_key)
        self.assertEqual(type(columns[0]), kida.IntField)

        self.assertEqual('fint', columns[1].name)
        self.assertFalse(columns[1].is_key)
        self.assertEqual(type(columns[1]), kida.IntField)

        self.assertEqual('fstr', columns[2].name)
        self.assertFalse(columns[2].is_key)
        self.assertEqual(type(columns[2]), kida.StringField)

        self.assertEqual('flong', columns[3].name)
        self.assertFalse(columns[3].is_key)
        self.assertEqual(type(columns[3]), kida.IntField)

        self.assertEqual('fdate', columns[4].name)
        self.assertFalse(columns[4].is_key)
        self.assertEqual(type(columns[4]), kida.DateField)

        self.assertEqual('fdatetime', columns[5].name)
        self.assertFalse(columns[5].is_key)
        self.assertEqual(type(columns[5]), kida.DatetimeField)

    def test_load_metadata_default_uniquekey(self):
        context = self.target
        table = context.load_metadata('users', key_type=kida.KEY_TYPE_UNIQUE_KEY)
        columns = table.columns
        self.assertEqual(len(columns), 2)

        # keys go first
        self.assertEqual('username', columns[0].name)
        self.assertTrue(columns[0].is_key)
        self.assertEqual(type(columns[0]), kida.StringField)

        self.assertEqual('id', columns[1].name)
        self.assertFalse(columns[1].is_key)
        self.assertEqual(type(columns[1]), kida.IntField)

            
    def test_load_metadata2(self):
        context = self.target
        table = context.load_metadata('table1')
        columns = table.columns
        self.assertTrue(columns['id'].is_key)
            
    def test_save2(self):
        context = self.target
        tablename = 'table1'
        context.load_metadata(tablename)
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
        context.load_metadata(tablename)
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
        tablename, context.load_metadata(tablename)
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
        context.load_metadata(tablename)
        for i in xrange(10000):
            data = {'id': i, "fint": 1, "fstr": 'ab\'c'}
            context.save(tablename, data)
            context.commit()

    def test_save_10000_batch(self):
        context = self.target
        tablename = 'table1'
        tablename, context.load_metadata(tablename)
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

        tablename, context.load_metadata(tablename)
        list = context._meta[tablename].fields
        self.assertEqual("k2", list[0].name, "The first field should be k2")
        self.assertEqual("k1", list[1].name, "The second field should be k1")
        
    def test_index_sequence_save(self):
        # since the primary keys order might not match the on of fields, it 
        # should as the sequence of indexes 
        context = self.target
        tablename = 'table2'
        
        data = {'k1' : 1, 'k2' : 2}

        tablename, context.load_metadata(tablename)
        context.save(tablename, data)
        context.commit()
        
    def test_exists_key(self):
        context = self.target
        tablename = 'table2'
        keys = {'k1' : 1, 'k2' : 2}

        tablename, context.load_metadata(tablename)
        context.save(tablename, keys)
        self.assertTrue(context.exists_key(tablename, keys)) 

    def test_not_existing_table_metadata(self):
        context = self.target
        tablename = 'table_not_existing'
        try:
            context.load_metadata(tablename)
            self.fail('TableNotExistsError not catched')
        except TableNotExistError:
            pass

    def test_external_meta(self):
        meta = kida.Meta()

        table_name = 'table1'
        try:
            self.assertFalse(meta[table_name])
            self.fail('Table1 should not exists in meta')
        except TableNotExistError:
            pass

        self.assertFalse(table_name in meta.tables)
        target = kida.connect(target_dburl, meta=meta)
        target.load_metadata(table_name)
        table = meta[table_name]


class DBConnectionTest(unittest.TestCase):
    def test_connect_by_url(self):
        dburl = 'mysql://pydb_test:password@localhost/pydb_test'
        db_context = kida.MySQLContext(dburl)
        db_context.close()

    def test_connect_by_url_plus_user(self):
        dburl = 'mysql://localhost/pydb_test'
        db_context = kida.MySQLContext(dburl, user='pydb_test', passwd='password')
        db_context.close()

    def test_connect_by_url_no_username(self):
        dburl = 'mysql://root@localhost/pydb_test'
        db_context = kida.MySQLContext(dburl)
        db_context.close()

    def test_connect_by_dict(self):
        params = {'user': 'pydb_test', 'passwd':'password', 'host': 'localhost', 'db': 'pydb_test'}
        db_context = kida.MySQLContext(params)
        db_context.close()

    def test_connect_by_kwargs(self):
        db_context = kida.MySQLContext(user='root', host='localhost', port=3306, db='pydb_test')
        db_context.close()

