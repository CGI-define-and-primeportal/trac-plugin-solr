import unittest

import fulltextsearchplugin
from fulltextsearchplugin.tests import fulltextsearch

def suite():
    suite = unittest.TestSuite()
    suite.addTest(fulltextsearch.suite())
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
