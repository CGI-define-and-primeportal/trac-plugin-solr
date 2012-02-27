import unittest

import fulltextsearchplugin
from fulltextsearchplugin.tests import fulltextsearch, admin

def suite():
    suite = unittest.TestSuite()
    suite.addTest(fulltextsearch.suite())
    suite.addTest(admin.suite())
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
