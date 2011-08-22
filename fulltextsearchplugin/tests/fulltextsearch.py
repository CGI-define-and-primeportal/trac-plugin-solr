import os
import shutil
import tempfile
import unittest

from trac.resource import Resource
from trac.test import EnvironmentStub, Mock

from fulltextsearchplugin.fulltextsearch import (FullTextSearchObject, Backend,
                                                 FullTextSearch,
                                                 )

class FullTextSearchObjectTestCase(unittest.TestCase):
    def setUp(self):
        self.project = 'project1'

    def test_create_defaults(self):
        so = FullTextSearchObject(self.project)
        self.assertEquals('project1', so.project)
        self.assertEquals(None, so.title)
        self.assertEquals(None, so.author)
        self.assertEquals(None, so.changed)
        self.assertEquals(None, so.created)
        self.assertEquals(None, so.oneline)
        self.assertEquals(None, so.tags)
        self.assertEquals(None, so.involved)
        self.assertEquals(None, so.popularity)
        self.assertEquals(None, so.body)
        self.assertEquals(None, so.action)
        self.assertEquals(False, hasattr(so, 'unknown'))

    def test_create_props(self):
        so = FullTextSearchObject(self.project,
                                  title='title', author='author',
                                  changed='changed', created='created',
                                  oneline='oneline',tags='tags',
                                  involved='involved', popularity='popularity',
                                  body='body', action='action',
                                  )
        self.assertEquals('project1', so.project)
        self.assertEquals('title', so.title)
        self.assertEquals('author', so.author)
        self.assertEquals('changed', so.changed)
        self.assertEquals('created', so.created)
        self.assertEquals('oneline', so.oneline)
        self.assertEquals('tags', so.tags)
        self.assertEquals('involved', so.involved)
        self.assertEquals('popularity', so.popularity)
        self.assertEquals('body', so.body)
        self.assertEquals('action', so.action)

    def test_create_unknown_raises(self):
        self.assertRaises(TypeError,
                          FullTextSearchObject, self.project, unknown='foo')

    def test_create_resource(self):
        so = FullTextSearchObject(self.project, Resource('wiki', 'WikiStart'))
        self.assertEquals('project1.wiki.WikiStart', so.id)
        self.assertEquals('wiki', so.realm)

    def test_create_realm(self):
        so = FullTextSearchObject(self.project, realm='bar', id='baz')
        self.assertEquals('project1.bar.baz', so.id)
        self.assertEquals('bar', so.realm)

    def test_create_resource_realm(self):
        so = FullTextSearchObject(self.project, Resource('wiki', 'WikiStart'),
                                  realm='bar', id='baz')
        self.assertEquals('project1.bar.baz', so.id)
        self.assertEquals('bar', so.realm)

    def test_create_resource_parent_realm(self):
        so = FullTextSearchObject(self.project,
                                  Resource('attachment', 'foo.txt'),
                                  parent_realm='wiki', parent_id='WikiStart')
        self.assertEquals('project1.attachment:wiki:WikiStart.foo.txt', so.id)
        self.assertEquals('attachment', so.realm)

    def test_create_resource_realm_parent_realm(self):
        so = FullTextSearchObject(self.project,
                                  Resource('attachment', 'foo.txt'),
                                  realm='bar', id='baz',
                                  parent_realm='wiki', parent_id='WikiStart')
        self.assertEquals('project1.bar:wiki:WikiStart.baz', so.id)
        self.assertEquals('bar', so.realm)

class FullTextSearchTestCase(unittest.TestCase):
    def setUp(self):
        self.env = EnvironmentStub(enable=['trac.*', FullTextSearch])
        self.env.path = os.path.join(tempfile.gettempdir(), 'trac-tempenv')
        os.mkdir(self.env.path)

        self.env.config.set('search', 'solr_endpoint', '')
        self.fts = FullTextSearch(self.env)

    def tearDown(self):
        shutil.rmtree(self.env.path)
        self.env.reset_db()

    def test_properties(self):
        self.assertEquals('trac-tempenv', self.fts.project)

    def _get_so(self):
        return self.fts.backend.get(block=False)
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FullTextSearchObjectTestCase, 'test'))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
