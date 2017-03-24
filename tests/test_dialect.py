'''
'''
import unittest
from kida.DbContext import Dialect
import datetime
import kida
import time

class Test(unittest.TestCase):


    def setUp(self):
        self.target = Dialect()


    def tearDown(self):
        pass


    def test_datetime_formating(self):
        value = datetime.datetime(2012,3,4, 16,39,43)
        ret = self.target.format_value_string(kida.DatetimeField("SomeField"), value)
        self.assertEqual("'2012-03-04 16:39:43'", ret, "Formating invalid")
        #print ret
        datetime.datetime.now()
    
    def test_datetime_formating2(self):
        value = datetime.date(2012, 3, 4)
        ret = self.target.format_value_string(kida.DatetimeField("SomeField"), value)
        self.assertEqual("'2012-03-04 00:00:00'", ret, "Formating invalid")
        #print ret
        
    def test_datetime_formating3(self):
        value = time.time()
        ret = self.target.format_value_string(kida.DatetimeField("SomeField"), value)
        print ret
