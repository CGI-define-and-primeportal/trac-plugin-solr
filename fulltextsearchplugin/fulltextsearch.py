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
    
class IFullTextSearchSource(ISearchSource):
    pass

class FullTextSearchModule(SearchModule):
    search_sources = ExtensionPoint(IFullTextSearchSource)

class FullTextSearchObject(object):

    title      = None
    author     = None
    changed    = None
    created    = None
    oneline    = None
    realm      = None
    tags       = None
    involved   = None
    popularity = None
    body       = None
    action     = None

    CREATE     = 'CREATE'
    MODIFY     = 'MODIFY'
    DELETE     = 'DELETE'

    def __init__(self, id, **kwargs):
        self.id = id
        # we can't just filter on the first part of id, because
        # wildcards are not supported by dismax in solr yet
        self.project = id.split(".",1)[0]

class Backend(Queue):
    """
    """

    def __init__(self, solr_endpoint):
        Queue.__init__(self)
        self.solr_endpoint = solr_endpoint

    def create(self, item):
        item.action = item.CREATE
        self.put(item)
        self.commit()
        
    def modify(self, item):
        item.action = item.MODIFY
        self.put(item)
        self.commit()
    
    def delete(self, item):
        item.action = item.DELETE
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
        s.delete(queries = "id:%s.*"%project_id) #I would have like some more info back
        s.commit()

    def commit(self):
        s = sunburnt.SolrInterface(self.solr_endpoint)
        while not self.empty():
            item = self.get()
            if item.action in (FullTextSearchObject.CREATE, 
                               FullTextSearchObject.MODIFY):
                if hasattr(item.body, 'read'):
                    s.add(item, extract=True)
                else:
                    s.add(item) #We can add multiple documents if we want
            elif item.action == FullTextSearchObject.DELETE:
                s.delete(item)
            else:
                raise Exception("Unknown solr action")
            s.commit()
        


class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener, IFullTextSearchSource, IAdminCommandProvider,
               IEnvironmentSetupParticipant)

    solr_endpoint = Option("search", "solr_endpoint",
                           default="http://localhost:8983/solr/",
                           doc="URL to use for HTTP REST calls to Solr")
    #Warning, sunburnt is case sensitive via lxml on xpath searches while solr is not
    #in the default schema fieldType and fieldtype mismatch gives problem
    def __init__(self):
        self.backend = Backend(self.solr_endpoint)

    def _unique_id(self, resource = None, realm = None, id = None):
        project_id = os.path.split(self.env.path)[1]
        if resource:
            id = resource.id
            realm = resource.realm
        unique_id = u"%s.%s.%s"%(project_id, realm, id)
        return unique_id
    
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
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        cursor.execute("SELECT type,id,filename,description,size,time,author,ipnr "
                       "FROM attachment")
        for parent_realm, parent_id, filename, description, size, time, author, ipnr in cursor:
            attachment = Attachment(self.env, parent_realm, parent_id)
            attachment.filename = filename
            attachment.description = description
            attachment.size = size and int(size) or 0
            attachment.date = datefmt.from_utimestamp(time or 0)
            attachment.author = author
            attachment.ipnr = ipnr
            self.attachment_added(attachment)

    def _reindex_ticket(self):
        db = self.env.get_db_cnx()
        cursor = db.cursor()
        cursor.execute("SELECT id FROM ticket")
        for (id,) in cursor:
            self.ticket_created(Ticket(self.env, id))

    def _reindex_milestone(self):
        for milestone in Milestone.select(self.env):
            self.milestone_created(milestone)
            
    def reindex(self):
        project_id = os.path.split(self.env.path)[1]
        self.backend.empty_proj(project_id)
        num_milestone = self._reindex_milestone()
        num_tickets = self._reindex_ticket()
        num_attachement = self._reindex_attachment()
        num_svn = self._reindex_svn()
        num_wiki = self._reindex_wiki()
        return num_svn

    # IEnvironmentSetupParticipant methods
    def environment_created(self):
        pass

    def environment_needs_upgrade(self, db):
       cursor = db.cursor()
       cursor.execute("SELECT value FROM system WHERE name = 'fulltextsearch_last_fullindex'")
       result = cursor.fetchone()
       if result is None:
           return True

    def upgrade_environment(self, db):
        cursor = db.cursor()
        t = to_utimestamp(datetime.now(utc))
        cursor.execute("INSERT INTO system (name, value) VALUES ('fulltextsearch_last_fullindex',%s)" % t)
        self.reindex()

    # ITicketChangeListener methods
    def ticket_created(self, ticket):
        ticketsystem = TicketSystem(self.env)
        so = FullTextSearchObject(self._unique_id(ticket.resource))
        so.title = "%(title)s: %(message)s"%{
                        'title':get_resource_shortname(self.env, ticket.resource),
                        'message':ticketsystem.get_resource_description(ticket.resource, format='summary')}
        so.author = ticket.values.get('reporter',None)
        so.changed = ticket.values.get('changetime', None)
        so.created = ticket.values.get('time', None)
        so.realm = ticket.resource.realm
        so.tags = ticket.values.get('keywords', None)
        so.involved = 'cc' in ticket.values and re.split(r'[;,\s]+', ticket.values['cc'])
        if not so.involved:
            so.involved = so.author
        so.popularity = 0 #FIXME
        so.oneline = shorten_result(ticket.values.get('description', ''))
        so.body = repr(ticket.values) + ' '.join([t[4] for t in ticket.get_changelog()])
        self.backend.create(so)
        self.log.debug("Ticket added for indexing: %s"%(ticket))
        
    def ticket_changed(self, ticket, comment, author, old_values):
        self.ticket_created(ticket)

    def ticket_deleted(self, ticket):
        so = FullTextSearchObject(self._unique_id(ticket.resource))
        self.backend.delete(so)
        self.log.debug("Ticket deleted; deleting from index: %s"%(ticket))

    #IWikiChangeListener methods
    def wiki_page_added(self, page):
        so = FullTextSearchObject(self._unique_id(page.resource))
        so.title = '%s: %s' % (page.name, shorten_line(page.text))
        so.author = page.author
        so.changed = page.time
        so.created = page.time #FIXME get time for version 1
        so.realm = page.resource.realm
        so.tags = None #FIXME 
        so.involved = () #FIXME get author and comment authors
        so.popularity = 0 #FIXME
        so.oneline = shorten_result(page.text)
        so.body = page.text #FIXME add comments as well
        self.backend.create(so)
        self.log.debug("WikiPage created for indexing: %s"%(page.name))

    def wiki_page_changed(self, page, version, t, comment, author, ipnr):
        self.wiki_page_added(page)

    def wiki_page_deleted(self, page):
        so = FullTextSearchObject(self._unique_id(page.resource))
        self.backend.delete(so)

    def wiki_page_version_deleted(self, page):
        #We don't care about old versions
        pass

    def wiki_page_renamed(page, old_name): 
        so = FullTextSearchObject(self_unique_id(page.resource))
        so.id = so.id.replace(page.name, old_name) #FIXME, can mess up
        self.backend.delete(so)
        self.wiki_page.added(page)

    #IAttachmentChangeListener methods
    def attachment_added(self, attachment):
        """Called when an attachment is added."""
        realm = u"%s:%s:%s" % (attachment.resource.realm, 
                               attachment.parent_realm, 
                               attachment.parent_id)
        so = FullTextSearchObject(self._unique_id(realm=realm, id=attachment.resource.id))
        so.realm = attachment.resource.realm
        so.title = attachment.title
        so.author = attachment.author
        so.changed = attachment.date
        so.created = attachment.date
        so.body = attachment.open()
        so.involved = attachment.author
        self.backend.create(so)

    def attachment_deleted(self, attachment):
        """Called when an attachment is deleted."""
        so = FullTextSearchObject(self._unique_id(attachment.resource))
        self.backend.delete(so)

    def attachment_reparented(self, attachment, old_parent_realm, old_parent_id):
        """Called when an attachment is reparented."""
        self.attachment_added(attachment)

    #IMilestoneChangeListener methods
    def milestone_created(self, milestone):
        so = FullTextSearchObject(self._unique_id(milestone.resource))
        so.title = '%s: %s' % (milestone.name, shorten_line(milestone.description))
        so.changed = (milestone.completed or milestone.due or datetime.now(datefmt.utc))
        so.realm = milestone.resource.realm
        so.involved = () #FIXME 
        so.popularity = 0 #FIXME
        so.oneline = shorten_result(milestone.description)
        so.body = milestone.description #FIXME add comments as well
        self.backend.create(so)
        self.log.debug("Milestone created for indexing: %s"%(milestone))

    def milestone_changed(self, milestone, old_values):
        """
        `old_values` is a dictionary containing the previous values of the
        milestone properties that changed. Currently those properties can be
        'name', 'due', 'completed', or 'description'.
        """
        self.milestone_created(milestone)

    def milestone_deleted(self, milestone):
        """Called when a milestone is deleted."""
        so = FullTextSearchObject(self._unique_id(milestone.resource))
        self.backend.delete(so)

    def _fill_so(self, node):
        so = FullTextSearchObject(self._unique_id(realm='versioncontrol', id=node.path))
        so.title   = node.path
        so.realm   = 'versioncontrol'
        so.body    = node.get_content()
        so.changed = node.get_last_modified()
        so.action  = so.CREATE
        return so

    #IRepositoryChangeListener methods
    def changeset_added(self, repos, changeset):
        """Called after a changeset has been added to a repository."""
        sos = []
        for (path, kind, action, base_path, base_rev) in changeset.get_changes():
            #FIXME handle kind == Node.DIRECTORY
            if action in (Changeset.ADD, Changeset.EDIT, Changeset.COPY):
                so = self._fill_so(repos.get_node(path, changeset.rev))
                sos.append(so)
            elif action == Changeset.MOVE:
                so = FullTextSearchObject(realm='versioncontrol', id=base_path)
                so.action = so.DELETE
                sos.append(so)
                so = self._fill_so(repos.get_node(path, changeset.rev))
                sos.append(sos)
            elif action == Changeset.DELETE:
                so = FullTextSearchObject(realm='versioncontrol', id=path)
                so.action = so.DELETE
                sos.append(sos)
        for so in sos:
            self.log.debug("Indexing: %s"%so.title)
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
        yield ('ticket', 'Tickets', True)
        yield ('wiki', 'Wiki', True)
        yield ('milestone', 'Milestones', True)
        yield ('changeset', 'Changesets', True)
        yield ('versioncontrol', 'File archive', True)
        yield ('attachment', 'Attachments', True)

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
        project_id = os.path.split(self.env.path)[1]        
        filter_q = self._build_filter_query(si, filters) & si.query().Q(project=project_id)
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
                # FIXME i18n
                if realm.split(":")[1] == "wiki":
                    title = "%s (attached to page %s)" % (rid, realm.split(":")[2])
                if realm.split(":")[1] == "ticket":
                    title = "%s (attached to ticket #%s)" % (rid, realm.split(":")[2])
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

