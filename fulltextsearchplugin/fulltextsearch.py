from Queue import Queue
import os
from genshi.builder import tag
from datetime import datetime
import re
import sunburnt
import types

from trac.env import IEnvironmentSetupParticipant
from trac.admin import AdminCommandError, IAdminCommandProvider
from trac.core import Component, implements, TracError, Interface
from trac.web.chrome import add_stylesheet
from trac.ticket.api import ITicketChangeListener, IMilestoneChangeListener, TicketSystem
from trac.ticket.model import Ticket, Milestone
from trac.wiki.api import IWikiChangeListener, WikiSystem
from trac.wiki.model import WikiPage
from trac.util.text import shorten_line
from trac.attachment import IAttachmentChangeListener, Attachment
from trac.versioncontrol.api import IRepositoryChangeListener, Changeset, Node
from trac.core import ExtensionPoint
from trac.resource import get_resource_name, get_resource_shortname
from trac.search import ISearchSource, shorten_result
from trac.util.translation import _, tag_
from trac.config import Option
from trac.util import datefmt
from trac.search.web_ui import SearchModule
from trac.util.datefmt import from_utimestamp, to_utimestamp, utc

__all__ = ['IFullTextSearchSource', 'FullTextSearchModule',
           'FullTextSearchObject', 'Backend', 'FullTextSearch',
           ]

class IFullTextSearchSource(Interface):
    pass

class FullTextSearchModule(Component):
    pass

class FullTextSearchObject(object):
    def __init__(self, project, resource=None, realm=None, id=None,
                 parent_realm=None, parent_id=None,
                 title=None, author=None, changed=None, created=None,
                 oneline=None, tags=None, involved=None,
                 popularity=None, body=None, action=None):
        # we can't just filter on the first part of id, because
        # wildcards are not supported by dismax in solr yet
        if resource and realm is None:
            realm = resource.realm
            id = resource.id
        self.project = project
        self.realm = realm
        if parent_realm and parent_id:
            self.id = u"%s.%s:%s:%s.%s" % (project, realm, parent_realm,
                                           parent_id, id)
        else:
            self.id = u"%s.%s.%s" % (project, realm, id)

        self.title = title
        self.author = author
        self.changed = changed
        self.created = created
        self.oneline = oneline
        self.tags = tags
        self.involved = involved
        self.popularity = popularity
        self.body = body
        self.action = action


class Backend(Queue):
    """
    """

    def __init__(self, solr_endpoint, log):
        Queue.__init__(self)
        self.log = log
        self.solr_endpoint = solr_endpoint

    def create(self, item):
        item.action = 'CREATE'
        self.put(item)
        self.commit()
        
    def modify(self, item):
        item.action = 'MODIFY'
        self.put(item)
        self.commit()
    
    def delete(self, item):
        item.action = 'DELETE'
        self.put(item)
        self.commit()

    def add(self, item):
        if isinstance(item, list):
            for i in item:
                self.put(i)
        else:
            self.put(item)
        self.commit()
        
    def empty_proj(self, project_id):
        s = sunburnt.SolrInterface(self.solr_endpoint)
        # I would have like some more info back
        s.delete(queries = u"id:%s.*" % project_id)
        s.commit()

    def commit(self):
        s = sunburnt.SolrInterface(self.solr_endpoint)
        while not self.empty():
            item = self.get()
            if item.action in ('CREATE', 'MODIFY'):
                if hasattr(item.body, 'read'):
                    s.add(item, extract=True)
                else:
                    s.add(item) #We can add multiple documents if we want
            elif item.action == 'DELETE':
                s.delete(item)
            else:
                raise Exception("Unknown solr action")
            try:
                s.commit()
            except Exception:
                self.log.error('%s %r', item, item)
                raise
                
        


class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener, ISearchSource, IAdminCommandProvider,
               IEnvironmentSetupParticipant)

    solr_endpoint = Option("search", "solr_endpoint",
                           default="http://localhost:8983/solr/",
                           doc="URL to use for HTTP REST calls to Solr")
    #Warning, sunburnt is case sensitive via lxml on xpath searches while solr is not
    #in the default schema fieldType and fieldtype mismatch gives problem
    def __init__(self):
        self.backend = Backend(self.solr_endpoint, self.log)
        self.project = os.path.split(self.env.path)[1]

    def _reindex_svn(self):
        class MockChangeset(list):
            def get_changes(self):
                return self
        repo = self.env.get_repository()
        mc = MockChangeset()
        mc.rev = repo.youngest_rev
        for path in repo.traverse():
            if path.endswith('/'):
                continue
            mc.append((path, Node.FILE, Changeset.ADD, None, -1))
        self.changeset_added(repo, mc)
        return len(mc)

    def _reindex_wiki(self):
        for name in WikiSystem(self.env).get_pages():
            page = WikiPage(self.env, name)
            self.wiki_page_added(page)

    def _reindex_attachment(self):
        db = self.env.get_read_db()
        cursor = db.cursor()
        # This plugin was originally written for #define 4, a Trac derivative
        # that includes versioned attachments. TO try and keep compatibility
        # with both check support by checking for a version attribute on an
        # Attachment. Instantiating Attachment doesn't perform any queries,
        # so it doesn't matter if ticket:42 actually exists
        # The versioned attachment code used by #define is published on github
        # https://github.com/moreati/trac-gitsvn/tree/0.12-versionedattachments
        canary = Attachment(self.env, 'ticket', 42)
        if hasattr(canary, 'version'):
            # Adapted from Attachment.select()
            cursor.execute("""
                SELECT type, id, filename, version, description, size, time,
                       author, ipnr, status, deleted
                FROM attachment
                JOIN (SELECT type AS c_type, id AS c_id,
                             filename AS c_filename, MAX(version) AS c_version
                      FROM attachment
                      WHERE deleted IS NULL
                      GROUP BY c_type, c_id, c_filename) AS current
                     ON type = c_type AND id = c_id
                        AND filename = c_filename AND version = c_version
                ORDER BY time""")
        else:
             cursor.execute(
                "SELECT type,id,filename,description,size,time,author,ipnr "
                "FROM attachment"
                )
        for row in cursor:
            parent_realm, parent_id = row[0], row[1]
            attachment = Attachment(self.env, parent_realm, parent_id)
            attachment._from_database(*row[2:])
            self.attachment_added(attachment)

    def _reindex_ticket(self):
        db = self.env.get_read_db()
        cursor = db.cursor()
        cursor.execute("SELECT id FROM ticket")
        for (id,) in cursor:
            self.ticket_created(Ticket(self.env, id))

    def _reindex_milestone(self):
        for milestone in Milestone.select(self.env):
            self.milestone_created(milestone)
            
    def reindex(self):
        self.backend.empty_proj(self.project)
        num_milestone = self._reindex_milestone()
        num_tickets = self._reindex_ticket()
        num_attachement = self._reindex_attachment()
        num_svn = self._reindex_svn()
        num_wiki = self._reindex_wiki()
        return num_svn

    # IEnvironmentSetupParticipant methods
    def environment_created(self):
        self.env.with_transaction()
        def do_upgrade(db):
            self.upgrade_environment(db)

    def environment_needs_upgrade(self, db):
       cursor = db.cursor()
       cursor.execute("SELECT value FROM system WHERE name = %s",
                      ('fulltextsearch_last_fullindex',))
       result = cursor.fetchone()
       if result is None:
           return True

    def upgrade_environment(self, db):
        cursor = db.cursor()
        self.reindex()
        t = to_utimestamp(datetime.now(utc))
        cursor.execute("INSERT INTO system (name, value) VALUES (%s,%s)",
                       ('fulltextsearch_last_fullindex', t))

    # ITicketChangeListener methods
    def ticket_created(self, ticket):
        ticketsystem = TicketSystem(self.env)
        resource_name = get_resource_shortname(self.env, ticket.resource)
        resource_desc = ticketsystem.get_resource_description(ticket.resource,
                                                              format='summary')
        so = FullTextSearchObject(
                self.project, ticket.resource,
                title = u"%(title)s: %(message)s" % {'title': resource_name,
                                                     'message': resource_desc},
                author = ticket.values.get('reporter'),
                changed = ticket.values.get('changetime'),
                created = ticket.values.get('time'),
                tags = ticket.values.get('keywords'),
                involved = re.split(r'[;,\s]+', ticket.values.get('cc', ''))
                           or ticket.values.get('reporter'),
                popularity = 0, #FIXME
                oneline = shorten_result(ticket.values.get('description', '')),
                body = u'%r%s' % (ticket.values,
                                  u' '.join(t[4] for t in ticket.get_changelog()),
                                  ),
                )
        self.backend.create(so)
        self.log.debug("Ticket added for indexing: %s", ticket)
        
    def ticket_changed(self, ticket, comment, author, old_values, action=None):
        self.ticket_created(ticket)

    def ticket_deleted(self, ticket):
        so = FullTextSearchObject(self.project, ticket.resource)
        self.backend.delete(so)
        self.log.debug("Ticket deleted; deleting from index: %s", ticket)

    #IWikiChangeListener methods
    def wiki_page_added(self, page):
        history = list(page.get_history())
        so = FullTextSearchObject(
                self.project, page.resource,
                title = u'%s: %s' % (page.name, shorten_line(page.text)),
                author = page.author,
                changed = page.time,
                created = history[-1][1], # .time of oldest version
                tags = self._page_tags(page.resource.realm, page.name),
                involved = list(set(r[2] for r in history)),
                popularity = 0, #FIXME
                oneline = shorten_result(page.text),
                body = u'\n'.join([page.text] + [unicode(r[3]) for r in history]),
                )
        self.backend.create(so)
        self.log.debug("WikiPage created for indexing: %s", page.name)

    def wiki_page_changed(self, page, version, t, comment, author, ipnr):
        self.wiki_page_added(page)

    def wiki_page_deleted(self, page):
        so = FullTextSearchObject(self.project, page.resource)
        self.backend.delete(so)

    def wiki_page_version_deleted(self, page, version, author):
        #We don't care about old versions
        pass

    def wiki_page_renamed(self, page, old_name): 
        so = FullTextSearchObject(self.project, page.resource)
        so.id = so.id.replace(page.name, old_name) #FIXME, can mess up
        self.backend.delete(so)
        self.wiki_page.added(page)

    def _page_tags(self, realm, page):
        db = self.env.get_read_db()
        cursor = db.cursor()
        try:
            cursor.execute('SELECT tag FROM tags '
                           'WHERE tagspace=%s AND name=%s '
                           'ORDER BY tag',
                           (realm, page))
        except Exception, e:
            # No common ProgrammingError between database APIs
            # For reasons unknown sqlite3 raises OperationalError when a table
            # doesn't exist
            if e.__class__.__name__ in ('ProgrammingError',
                                        'OperationalError'):
                return []
            else:
                raise e
        return (tag for (tag,) in cursor)

    #IAttachmentChangeListener methods
    def attachment_added(self, attachment):
        """Called when an attachment is added."""
        if hasattr(attachment, 'version'):
            history = list(attachment.get_history())
            created = history[-1].date
            involved = list(set(a.author for a in history))
            comments = list(set(a.description for a in history 
                                if a.description))
        else:
            created = attachment.date
            involved = attachment.author
            comments = [attachment.description]
        so = FullTextSearchObject(
                self.project, attachment.resource,
                realm = attachment.resource.realm,
                parent_realm = attachment.parent_realm,
                parent_id = attachment.parent_id,
                id = attachment.resource.id,
                title = attachment.title,
                author = attachment.author,
                changed = attachment.date,
                created = created,
                # FIXME I think SOLR expects UTF-8, we give it a BLOB of
                #       arbitrary bytes
                body = attachment.open().read() + '\n'.join(comments),
                involved = involved,
                )
        self.backend.create(so)

    def attachment_deleted(self, attachment):
        """Called when an attachment is deleted."""
        so = FullTextSearchObject(self.project, attachment.resource)
        self.backend.delete(so)

    def attachment_reparented(self, attachment, old_parent_realm, old_parent_id):
        """Called when an attachment is reparented."""
        self.attachment_added(attachment)

    #IMilestoneChangeListener methods
    def milestone_created(self, milestone):
        so = FullTextSearchObject(
                self.project, milestone.resource,
                title = u'%s: %s' % (milestone.name,
                                     shorten_line(milestone.description)),
                changed = milestone.completed or milestone.due
                                              or datetime.now(datefmt.utc),
                involved = (),
                popularity = 0, #FIXME
                oneline = shorten_result(milestone.description),
                body = milestone.description,
                )
        self.backend.create(so)
        self.log.debug("Milestone created for indexing: %s", milestone)

    def milestone_changed(self, milestone, old_values):
        """
        `old_values` is a dictionary containing the previous values of the
        milestone properties that changed. Currently those properties can be
        'name', 'due', 'completed', or 'description'.
        """
        self.milestone_created(milestone)

    def milestone_deleted(self, milestone):
        """Called when a milestone is deleted."""
        so = FullTextSearchObject(self.project, milestone.resource)
        self.backend.delete(so)

    def _fill_so(self, node):
        so = FullTextSearchObject(
                self.project,
                realm = u'versioncontrol', id=node.path,
                title = node.path,
                body = node.get_content(),
                changed = node.get_last_modified(),
                action = 'CREATE',
                )
        return so

    #IRepositoryChangeListener methods
    def changeset_added(self, repos, changeset):
        """Called after a changeset has been added to a repository."""
        sos = []
        for path, kind, change, base_path, base_rev in changeset.get_changes():
            #FIXME handle kind == Node.DIRECTORY
            if change in (Changeset.ADD, Changeset.EDIT, Changeset.COPY):
                so = self._fill_so(repos.get_node(path, changeset.rev))
                sos.append(so)
            elif change == Changeset.MOVE:
                so = FullTextSearchObject(
                        self.project, realm=u'versioncontrol', id=base_path,
                        action='DELETE')
                sos.append(so)
                so = self._fill_so(repos.get_node(path, changeset.rev))
                sos.append(so)
            elif change == Changeset.DELETE:
                so = FullTextSearchObject(
                        self.project, realm=u'versioncontrol', id=path,
                        action='DELETE')
                sos.append(so)
        for so in sos:
            self.log.debug("Indexing: %s", so.title)
        self.backend.add(sos)

    def changeset_modified(self, repos, changeset, old_changeset):
        """Called after a changeset has been modified in a repository.

        The `old_changeset` argument contains the metadata of the changeset
        prior to the modification. It is `None` if the old metadata cannot
        be retrieved.
        """
        #Hmm, I wonder if this is called instead of the above method or after
        pass

    # ISearchSource methods.

    def get_search_filters(self, req):
        yield (u'ticket', u'Tickets', True)
        yield (u'wiki', u'Wiki', True)
        yield (u'milestone', u'Milestones', True)
        yield (u'changeset', u'Changesets', True)
        yield (u'versioncontrol', u'File archive', True)
        yield (u'attachment', u'Attachments', True)

    def _build_filter_query(self, si, filters):
        Q = si.query().Q
        my_filters = filters[:]
        for field in my_filters:
            if field not in [shortname for (shortname,t2,t3) in self.get_search_filters(None)]:
                my_filters.remove(field)
        def rec(list1):
            if len(list1) > 2:
                return Q(realm=list1.pop()) | rec(list1)
            elif len(list1) == 2:
                return Q(realm=list1.pop()) | Q(realm=list1.pop())
            elif len(list1) == 1:
                return Q(realm=list1.pop())
            else: 
                return ""
        return rec(my_filters[:])

    def get_search_results(self, req, terms, filters):
        self.log.debug("get_search_result called")
        try:
            si = sunburnt.SolrInterface(self.solr_endpoint)
        except:
            return #until solr is packaged
        filter_q = self._build_filter_query(si, filters) & si.query().Q(project=self.project)
        if self._has_wildcard(terms):
            self.log.debug("Found wildcard query, switching to standard parser")
            result = si.query(terms).filter(filter_q).facet_by('realm').execute()
        else:
            opts = {'q':terms,'qt':"dismax", 'facet':True, 'facet.field':"realm",
                    'fq':filter_q}
            result = si.search(**opts)
        self.log.debug("Facets: %s", result.facet_counts.facet_fields)
        for doc in result.result.docs:
            date = doc.get('changed', None)
            if date is not None:
                date = datetime.fromtimestamp((date._dt_obj.ticks()), tz=datefmt.localtz)  #if we get mx.datetime
                #date = date._dt_obj.replace(tzinfo=datefmt.localtz) # if we get datetime.datetime
            (proj,realm,rid) = doc['id'].split('.', 2)
            # try hard to get some 'title' which is needed for clicking on
            title   = doc.get('title', rid)
            if realm == 'versioncontrol':
                href = req.href('browser', rid)
            elif 'attachment:' in realm:    #FIXME hacky stuff here
                href = req.href(realm.replace(':','/'), rid)
                # FIXME is there a better way to do this?
                if realm.split(":")[1] == "wiki":
                    title = _(u"%(filename)s (attached to page %(wiki_page)s)",
                              filename=rid, wiki_page=realm.split(":")[2])
                if realm.split(":")[1] == "ticket":
                    title = _(u"%(filename)s (attached to ticket #%(ticket)s)",
                              filename=rid, ticket=realm.split(":")[2])
            else:
                href = req.href(realm, rid)
            author  = doc.get('author','')
            if isinstance(author,types.ListType):
                author = ", ".join(author)
            excerpt = doc.get('oneline','')
            yield (href, title, date, author, excerpt)

    def _has_wildcard(self, terms):
        for term in terms:
            if '*' in term:
                return True
        return False
    #IAdminCommandProvider methods
    def get_admin_commands(self):
        yield ('fulltext reindex', '',
               'Throw away everything in text index and add it again',
               self._complete_admin_command, self._admin_reindex)

    def _complete_admin_command(self, args):
        return []

    def _admin_reindex(self):
        self.reindex()
        print "reindex done"

