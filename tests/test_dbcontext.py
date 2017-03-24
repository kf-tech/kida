import kida
import unittest


dburl = 'mysql://root@localhost'

class Test(unittest.TestCase):
    def test_create_context(self):
        context = kida.create_context(dburl)
        context.close()

    def test_connect(self):
        context = kida.connect(dburl)
        context.close()