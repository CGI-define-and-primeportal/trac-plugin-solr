from Queue import Queue
import os
from genshi.builder import tag
from datetime import datetime
import re
import sunburnt

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


class Backend(Queue):
    """
    """

    def __init__(self, solr_endpoint, solr_schema):
        Queue.__init__(self)
        self.solr_endpoint = solr_endpoint
        self.solr_schema   = solr_schema

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
        s = sunburnt.SolrInterface(self.solr_endpoint, self.solr_schema)
        s.delete(queries = "id:%s.*"%project_id) #I would have like some more info back
        s.commit()

    def commit(self):
        try:
            s = sunburnt.SolrInterface(self.solr_endpoint, self.solr_schema)
            while not self.empty():
                item = self.get()
                if item.action in (FullTextSearchObject.CREATE, 
                                   FullTextSearchObject.MODIFY):
                    s.add(item) #We can add multiple documents if we want
                elif item.action == FullTextSearchObject.DELETE:
                    s.delete(item)
                else:
                    raise Exception("Unknown solr action")
                s.commit()
        except Exception, e:
            pass


class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener, ISearchSource, IAdminCommandProvider)

    solr_endpoint = Option("search", "solr_endpoint",
                           default="http://localhost:8080/solr",
                           doc="URL to use for HTTP REST calls to Solr")

    solr_schema = Option("search", "solr_schema",
                         default="/etc/solr/conf/schema.xml",
                         doc="Path to Solr schema XML")

    def __init__(self):
        self.backend = Backend(self.solr_endpoint, self.solr_schema)
        
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
        self.log.debug("Ticket added for indexing: %s %s"%(ticket,so))
        
    def ticket_changed(self, ticket, comment, author, old_values):
        self.ticket_created(ticket)

    def ticket_deleted(self, ticket):
        so = FullTextSearchObject(self._unique_id(ticket.resource))
        self.backend.delete(so)
        self.log.debug("Ticket deleted; deleting from index: %s %s"%(ticket,so))

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
        self.log.debug("WikiPage created for indexing: %s %s"%(page.name, so))

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
        so.body = attachment.open().read().decode('utf-8') + attachment.description
        so.oneline = shorten_line(so.body)
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
        self.log.debug("Milestone created for indexing: %s %s"%(milestone, so))

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
        so = FullTextSearchObject(self._unique_id(realm='browser', id=node.path))
        so.title   = node.path
        so.body    = node.get_content().read().decode('utf-8')
        so.changed = node.get_last_modified()
        so.action  = so.CREATE
        so.oneline = shorten_line(so.body)
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
                so = FullTextSearchObject(realm='browser', id=base_path)
                so.action = so.DELETE
                sos.append(so)
                so = self._fill_so(repos.get_node(path, changeset.rev))
                sos.append(sos)
            elif action == Changeset.DELETE:
                so = FullTextSearchObject(realm='browser', id=path)
                so.action = so.DELETE
                sos.append(sos)
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
        yield ('fulltext', 'Full text search', True)

    def get_search_results(self, req, terms, filters):
        self.log.debug("get_search_result called")
        if not 'fulltext' in filters:
            return
        try:
            si = sunburnt.SolrInterface(self.solr_endpoint, self.solr_schema)
        except:
            return #until solr is packaged 
        if self._has_wildcard(terms):
            self.log.debug("Found wildcard query, switching to standard parser")
            result = si.query(terms).execute().result
        else:
            result = si.search(q=terms,qt="dismax").result
        for doc in result.docs:
            date = doc.get('changed', None)
            if date is not None:
                date = datetime.fromtimestamp((date._dt_obj.ticks()), tz=datefmt.localtz)  #if we get mx.datetime
                #date = date._dt_obj.replace(tzinfo=datefmt.localtz) # if we get datetime.datetime
            (proj,realm,rid) = doc['id'].split('.', 2)
            if realm == 'versioncontrol':
                href = req.href('browser', rid)
            elif 'attachment:' in realm:    #FIXME hacky stuff here
                href = req.href(realm.replace(':','/'), rid)
            else:
                href = req.href(realm, rid)
            yield (href, doc.get('title',''), date, doc.get('author',''), doc.get('oneline',''))
    #IAdminCommandProvider methods
    def _has_wildcard(self, terms):
        for term in terms:
            if '*' in term:
                return True
        return False
    
    def get_admin_commands(self):
        yield ('fulltext reindex', '',
               'Throw away everything in text index and add it again',
               self._complete_admin_command, self._admin_reindex)

    def _complete_admin_command(self, args):
        return []

    def _admin_reindex(self):
        num = self.reindex()
        print "%d files added for reindexing."%num

