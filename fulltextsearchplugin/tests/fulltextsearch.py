from datetime import datetime, timedelta
from StringIO import StringIO
import os
import shutil
import tempfile
import unittest
import logging

from trac.attachment import Attachment
from trac.resource import Resource
from trac.test import EnvironmentStub, Mock
from trac.ticket import Ticket, Milestone
from trac.util.datefmt import from_utimestamp, to_utimestamp, utc
from trac.wiki import WikiPage

from fulltextsearchplugin.fulltextsearch import (FullTextSearchObject, Backend,
                                                 FullTextSearch,
                                                 )

class MockBackend(Backend):
    def __init__(self, endpoint):
        log = logging.getLogger('MockBackend')
        Backend.__init__(self, endpoint, log)
    def empty_proj(self):
        pass

    def commit(self):
        pass

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
        self.env.path = tempfile.mkdtemp(prefix='trac-testenv')
        self.basename = os.path.basename(self.env.path)
        #self.env.config.set('search', 'solr_endpoint', 'http://localhost:8983/solr/')
        self.fts = FullTextSearch(self.env)
        self.fts.backend = MockBackend(self.fts.solr_endpoint)

    def tearDown(self):
        shutil.rmtree(self.env.path)
        self.env.reset_db()

    def test_properties(self):
        self.assertEquals(self.basename, self.fts.project)

    def _get_so(self):
        return self.fts.backend.get(block=False)

    def test_attachment(self):
        attachment = Attachment(self.env, 'ticket', 42)
        attachment.description = 'Summary line'
        attachment.author = 'Santa'
        attachment.ipnr = 'northpole.example.com'
        attachment.insert('foo.txt', StringIO('Lorem ipsum dolor sit amet'), 0)
        so = self._get_so()
        self.assertEquals('%s.attachment:ticket:42.foo.txt' % self.basename, so.id)
        self.assertTrue('foo.txt' in so.title)
        self.assertEquals('Santa', so.author)
        self.assertEquals(attachment.date, so.created)
        self.assertEquals(attachment.date, so.changed)
        self.assertTrue('Santa' in so.involved)
        #self.assertTrue('Lorem ipsum' in so.oneline) # TODO
        self.assertTrue('Lorem ipsum' in so.body)
        self.assertTrue('Summary line' in so.body)

    def test_ticket(self):
        self.env.config.set('ticket-custom', 'foo', 'text')
        ticket = Ticket(self.env)
        ticket.populate({'reporter': 'santa', 'summary': 'Summary line',
                         'description': 'Lorem ipsum dolor sit amet',
                         'foo': 'This is a custom field',
                         'keywords': 'alpha bravo charlie',
                         'cc': 'a@b.com, c@example.com',
                         })
        ticket.insert()
        so = self._get_so()
        self.assertEquals('%s.ticket.1' % self.basename, so.id)
        self.assertTrue('#1' in so.title)
        self.assertTrue('Summary line' in so.title)
        self.assertEquals('santa', so.author)
        self.assertEquals(ticket['time'], so.created)
        self.assertEquals(ticket['changetime'], so.changed)
        self.assertTrue('a@b.com' in so.involved)
        self.assertTrue('c@example.com' in so.involved)
        self.assertTrue('bravo' in so.tags)
        self.assertTrue('Lorem ipsum' in so.oneline)
        self.assertTrue('Lorem ipsum' in so.body)

        original_time = ticket['time']
        ticket['description'] = 'No latin filler here'
        ticket.save_changes('Jack Sprat', 'Could eat no fat')
        so = self._get_so()
        self.assertEquals('%s.ticket.1' % self.basename, so.id)
        self.assertEquals(original_time, so.created)
        self.assertEquals(ticket['changetime'], so.changed)
        self.assertFalse('Lorem ipsum' in so.body)
        self.assertTrue('No latin filler here' in so.body)
        self.assertTrue('Could eat no fat' in so.body)

    def test_wiki_page(self):
        page = WikiPage(self.env, 'NewPage')
        page.text = 'Lorem ipsum dolor sit amet'
        # TODO Tags
        page.save('santa', 'Commment', 'northpole.example.com')
        so = self._get_so()
        self.assertEquals('%s.wiki.NewPage' % self.basename, so.id)
        self.assertTrue('NewPage' in so.title)
        self.assertTrue('Lorem ipsum' in so.title)
        self.assertEquals('santa', so.author)
        self.assertEquals(page.time, so.created)
        self.assertEquals(page.time, so.changed)
        self.assertTrue('santa' in so.involved)
        self.assertTrue('Lorem ipsum' in so.oneline)
        self.assertTrue('Lorem ipsum' in so.body)

        original_time = page.time
        page.text = 'No latin filler here'
        page.save('Jack Sprat', 'Could eat no fat', 'dinnertable.local')
        so = self._get_so()
        self.assertEquals('%s.wiki.NewPage' % self.basename, so.id)
        self.assertEquals(original_time, so.created)
        self.assertEquals(page.time, so.changed)
        self.assertFalse('Lorem ipsum' in so.body)
        self.assertTrue('No latin filler here' in so.body)
        self.assertTrue('Could eat no fat' in so.body)
    
    def test_wiki_page_unicode_error(self):
        import pkg_resources
        import define
        text = open(pkg_resources.resource_filename(define.__name__, 'default-pages/DefineGuide%2FTicketTutorial')).read()
        page = WikiPage(self.env, 'TicketTutorial')
        page.text = text.decode('utf-8')
        page.save('olle', 'Comment', 'define.logica.com')
        so = self._get_so()
        self.assertEquals('%s.wiki.TicketTutorial' % self.basename, so.id)
        
    def test_milestone(self):
        milestone = Milestone(self.env)
        milestone.name = 'New target date'
        milestone.description = 'Lorem ipsum dolor sit amet'
        milestone.insert()
        so = self._get_so()
        self.assertEquals('%s.milestone.New target date' % self.basename, so.id)
        self.assertTrue('New target date' in so.title)
        self.assertTrue('Lorem ipsum' in so.title)
        self.assertTrue('Lorem ipsum' in so.oneline)
        self.assertTrue('Lorem ipsum' in so.body)

        milestone.description = 'No latin filler here'
        milestone.due = datetime(2001, 01, 01, tzinfo=utc)
        milestone.update()
        so = self._get_so()
        self.assertEquals('%s.milestone.New target date' % self.basename, so.id)
        self.assertEquals(milestone.due, so.changed)
        self.assertFalse('Lorem ipsum' in so.body)
        self.assertTrue('No latin filler here' in so.body)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FullTextSearchObjectTestCase, 'test'))
    suite.addTest(unittest.makeSuite(FullTextSearchTestCase, 'test'))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
