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
from trac.versioncontrol.api import RepositoryManager, DbRepositoryProvider
from trac.loader import load_components
import pkg_resources
from trac.versioncontrol import svn_fs
from svn import core, repos
from trac_browser_svn_ops.svn_fs import SubversionWriter

class MockSolrInterface(object):
    """A bare minimum, in process simulation of sunburnt SolrInterface
    
    Submitted documents are stored as a class attribute, so all instances share
    the submitted documents. No transaction isolation. No schema enforcement.
    No document extraction. No indexing. Only equality searches,
    with a search string of the form `field:value` are implemented by .query().
    
    To aid testing a history of add/delete operations is maintained.
    """

    docs = {} # Committed documents - keyed by id, shared by all instanced
    hist = [] # History of operations - in the order committed

    def __init__(self, end_point):
        self.pending = []
        self.writable = True

    def _doc2docs(self, doc_or_docs):
        if hasattr(doc_or_docs, 'id'):
            docs = [doc_or_docs]
        elif doc_or_docs:
            docs = doc_or_docs
        else:
            docs = []
        return docs

    def add(self, docs, extract=False):
        docs = self._doc2docs(docs)
        for doc in docs:
            self.pending.append(('add', doc.id, doc))

    def delete(self, docs=None, queries=None):
        docs = self._doc2docs(docs)
        for query in queries or []:
            docs += self.query(query)
        for doc in docs:
            self.pending.append(('delete', doc.id, doc))

    def delete_all(self):
        for doc in self.docs:
            self.pending.append(('delete', doc.id, doc))

    def query(self, query):
        field, val = query.split(':', 1)
        return [doc for doc in self.docs.itervalues()
                    if getattr(doc, field, None) == val]

    def commit(self):
        for op, docid, doc in self.pending:
            if op == 'delete':
                self.docs.pop(docid, None)
            elif op == 'add':
                self.docs[docid] = doc
            self.hist.append((op,docid,doc))
        self.pending = []

    @classmethod
    def _reset(cls):
        cls.docs = {}
        cls.hist = []



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
        self.fts.backend = Backend(self.fts.solr_endpoint, self.env.log,
                                   MockSolrInterface)

    def tearDown(self):
        shutil.rmtree(self.env.path)
        self.env.reset_db()

    def test_properties(self):
        self.assertEquals(self.basename, self.fts.project)

    def _get_so(self):
        si = self.fts.backend.si_class(self.fts.backend.solr_endpoint)
        return si.hist[-1][2]

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
        self.assertTrue('Lorem ipsum' in so.body.read())
        self.assertTrue('Summary line' in so.comments)

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
        self.assertTrue('Could eat no fat' in so.comments)
        

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
        self.assertTrue('Could eat no fat' in so.comments)
    
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


class ChangesetsSvnTestCase(unittest.TestCase):
    @classmethod
    def setupClass(cls):
        svn_fs._import_svn()
        core.apr_initialize()
        pool = core.svn_pool_create(None)
        dumpstream = None
        cls.repos_path = tempfile.mkdtemp(prefix='svn-tmp')
        shutil.rmtree(cls.repos_path)
        dumpfile = open(os.path.join(os.path.split(__file__)[0], 'svn.dump'))
        try:
            r = repos.svn_repos_create(cls.repos_path, '', '', None, None, pool)
            if hasattr(repos, 'svn_repos_load_fs2'):
                repos.svn_repos_load_fs2(r, dumpfile, StringIO(),
                                        repos.svn_repos_load_uuid_default, '',
                                        0, 0, None, pool)
            else:
                dumpstream = core.svn_stream_from_aprfile(dumpfile, pool)
                repos.svn_repos_load_fs(r, dumpstream, None,
                                        repos.svn_repos_load_uuid_default, '',
                                        None, None, pool)
        finally:
            if dumpstream:
                core.svn_stream_close(dumpstream)
            core.svn_pool_destroy(pool)
            core.apr_terminate()
    @classmethod
    def teardownClass(cls):
        if os.name == 'nt':
            # The Windows version of 'shutil.rmtree' doesn't override the
            # permissions of read-only files, so we have to do it ourselves:
            import stat
            format_file = os.path.join(cls.repos_path, 'db', 'format')
            if os.path.isfile(format_file):
                os.chmod(format_file, stat.S_IRWXU)
            os.chmod(os.path.join(cls.repos_path, 'format'), stat.S_IRWXU)
        shutil.rmtree(cls.repos_path)
    
    def setUp(self):
        self.env = EnvironmentStub(enable=['trac.*', FullTextSearch])
        DbRepositoryProvider(self.env).add_repository('', self.repos_path, 'svn')
        self.repos = self.env.get_repository('')
        self.repos.sync()
        self.fts = FullTextSearch(self.env)
        self.fts.backend = Backend(self.fts.solr_endpoint, self.env.log,
                                   MockSolrInterface)
        
    def tearDown(self):
        self.env.reset_db()
        self.repos.close()
        self.repos = None
    
    def _get_so(self):
        si = self.fts.backend.si_class(self.fts.backend.solr_endpoint)
        return si.hist[-1][2]

    def test_reindex_svn(self):
        self.assertEquals(self.repos.youngest_rev, self.fts._reindex_svn())
    
    def test_add_changeset(self):
        sw = SubversionWriter(self.env, self.repos, 'kalle')
        new_rev = sw.put_content('/trunk/foo.txt', content='Foo Bar', commit_msg='A comment')
        RepositoryManager(self.env).notify('changeset_added', '', [new_rev])
        so = self._get_so()
        self.assertEquals(so.body.read(), 'Foo Bar')
        self.assertTrue('A comment' in so.comments)
        self.assertTrue('foo.txt' in so.id)
        self.assertTrue(str(self.repos.youngest_rev) in so.oneline)
        self.assertTrue('kalle' in so.author)
        
def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(FullTextSearchObjectTestCase, 'test'))
    suite.addTest(unittest.makeSuite(FullTextSearchTestCase, 'test'))
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
