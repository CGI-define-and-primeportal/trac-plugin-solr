import difflib
import os
import sys
import unittest
from StringIO import StringIO

from trac.admin import console
from trac.admin.tests.console import (STRIP_TRAILING_SPACE,
                                      load_expected_results)
from trac.test import EnvironmentStub

from fulltextsearchplugin.admin import FullTextSearchAdmin
from fulltextsearchplugin.fulltextsearch import Backend, FullTextSearch
from fulltextsearchplugin.tests.fulltextsearch import MockSolrInterface

__all__ = ['FullTextSearchAdminTestCase', 'suite']

class FullTextSearchAdminTestCase(unittest.TestCase):
    expected_results = load_expected_results(
            os.path.join(os.path.split(__file__)[0], 'console-tests.txt'),
            '===== (test_[^ ]+) =====')

    def setUp(self):
        self.env = EnvironmentStub('', enable=['trac.*', FullTextSearch,
                                               FullTextSearchAdmin])
        self.db = self.env.get_db_cnx()

        self._admin = console.TracAdmin()
        self._admin.env_set('', self.env)

        self.fts = FullTextSearch(self.env)
        self.fts.backend = Backend(self.fts.solr_endpoint, self.env.log,
                                   MockSolrInterface)

    def tearDown(self):
        MockSolrInterface._reset()
        self.env.reset_db()

    def _execute(self, cmd, strip_trailing_space=True):
        _err = sys.stderr
        _out = sys.stdout
        try:
            sys.stderr = sys.stdout = out = StringIO()
            setattr(out, 'encoding', 'utf-8') # fake output encoding
            retval = None
            try:
                retval = self._admin.onecmd(cmd)
            except SystemExit, e:
                pass
            value = out.getvalue()
            if isinstance(value, str): # reverse what print_listing did
                value = value.decode('utf-8')
            # DEBUG: uncomment in case of `AssertionError: 0 != 2` in tests
            #if retval != 0:
            #    print>>_err, value
            if strip_trailing_space:
                return retval, STRIP_TRAILING_SPACE.sub('', value)
            else:
                return retval, value
        finally:
            sys.stderr = _err
            sys.stdout = _out

    def assertEqual(self, expected_results, output):
        if not (isinstance(expected_results, basestring) and \
                isinstance(output, basestring)):
            return unittest.TestCase.assertEqual(self, expected_results, output)
        # Create a useful delta between the output and the expected output
        output_lines = ['%s\n' % x for x in output.split('\n')]
        expected_lines = ['%s\n' % x for x in expected_results.split('\n')]
        output_diff = ''.join(list(
            difflib.unified_diff(expected_lines, output_lines,
                                 'expected', 'actual')
        ))
        unittest.TestCase.assertEqual(self, expected_results, output, 
                                      "%r != %r\n%s" %
                                      (expected_results, output, output_diff))

    def test_reindex_milestone(self):
        test_name = sys._getframe().f_code.co_name
        expected = self.expected_results[test_name]
        rv, output = self._execute('fulltext reindex milestone')
        self.assertEqual(expected, output)
        self.assertEqual(0, rv)

    def test_reindex_unknown(self):
        test_name = sys._getframe().f_code.co_name
        expected = self.expected_results[test_name]
        rv, output = self._execute('fulltext reindex unknown_realm')
        self.assertEqual(expected, output)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FullTextSearchAdminTestCase, 'test'))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
