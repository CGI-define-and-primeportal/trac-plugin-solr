from Queue import Queue
import os
from trac.core import Component, implements, TracError, Interface
from trac.web.chrome import add_stylesheet
from trac.ticket.api import ITicketChangeListener, IMilestoneChangeListener
from trac.wiki.api import IWikiChangeListener
from trac.attachment import IAttachmentChangeListener
from trac.versioncontrol.api import IRepositoryChangeListener
from trac.core import ExtensionPoint
from trac.resource import get_resource_name
from pkg_resources import resource_filename
import re

class FullTextSearchObject(dict):
    possible_fields = ('id', 'author', 'changed', 'created', 'oneline', 'realm', 
                       'tags', 'involved', 'body', 'popularity')

    def __init__(self, id, **kwargs):
        self.id = id
        self.__dict__.update(kwargs)

class Backend(Queue):
    """
    """

    def create(self, item):
        item.action = 'CREATE'
        self.put(item)
        
    def modify(self, id, item):
        item.action = 'MODIFY'
        self.put(item)
    
    def delete(self, id, item):
        item.action = 'DELETE'
        self.put(item)
        
class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener)
    
    def __init__(self):
        self.backend = Backend()
        
    def _unique_id(self, resource):
        project_id = os.path.split(self.env.path)[1]
        unique_id = u"%s.%s.%s"%(project_id, resource.realm, resource.id)
        return unique_id
    
    # ITicketChangeListener methods
    def ticket_created(self, ticket):
        so = FullTextSearchObject(self._unique_id(ticket.resource))
        so.author = ticket.values.get('reporter',None)
        so.changed = ticket.values.get('changetime', None)
        so.created = ticket.values.get('changetime', None)
        so.realm = ticket.resource.realm
        so.tags = ticket.values.get('keywords', None)
        so.involved = 'cc' in ticket.values and re.split(r'[;,\s]+', ticket.values['cc'])
        if not so.involved:
            so.involved = so.author
        so.popularity = 0 #FIXME
        so.body = ticket.values
        self.backend.create(so)
        self.log.debug("Ticket added for indexing: %s %s"%(ticket,so))

    def ticket_changed(self, ticket, comment, author, old_values):
        so = FullTextSearchObject(self._unique_id(ticket.resource))
        so.changed = ticket.values.get('changetime', None)
        so.tags = ticket.values.get('keywords', None)
        so.involved = ()#FIXME
        so.popularity = 0 #FIXME
        self.backend.modify(self, so)
        self.log.debug("Ticket changed; updating full text index: %s %s"%(ticket,so))

    def ticket_deleted(self, ticket):
        so = FullTextSearchObject(self._unique_id(ticket.resource))
        self.backend.delete(so)
        self.log.debug("Ticket deleted; deleting from index: %s %s"%(ticket,so))
        
    #IWikiChangeListener methods
    def wiki_page_added(self, page):
        so = FullTextSearchObject(self._unique_id(page.resource))
        so.author = page.author
        so.changed = page.time
        so.created = page.time
        so.realm = page.resource.realm
        so.tags = None #FIXME
        so.involved = () #FIXME
        so.popularity = 0 #FIXME
        self.backend.create(so)
        self.log.debug("WikiPage created for indexing: %s %s"%(page, so))
        
    def wiki_page_changed(self, page, version, t, comment, author, ipnr):
        so = FullTextSearchObject(self._unique_id(page.resource))
        so.changed = page.t
        so.tags = None #FIXME
        so.involved = () #FIXME author change
        self.backend.create(so)
        self.log.debug("WikiPage changed; updating full text index: %s %s"%(page, so))

    def wiki_page_deleted(self, page):
        so = FullTextSearchObject(self._unique_id(page.resource))
        self.backend.delete(so)
        
    def wiki_page_version_deleted(self, page):
        #We don't care about old versions
        pass

    def wiki_page_renamed(page, old_name): 
#        so = FullTextSearchObject(page.resource.get_unique_id())
        #delete and create
        pass #FIXME

    #IAttachmentChangeListener methods
    def attachment_added(self, attachment):
        """Called when an attachment is added."""
        pass

    def attachment_deleted(self, attachment):
        """Called when an attachment is deleted."""
        pass

    def attachment_reparented(self, attachment, old_parent_realm, old_parent_id):
        """Called when an attachment is reparented."""
        pass
    
    #IMilestoneChangeListener methods
    def milestone_created(self, milestone):
        pass

    def milestone_changed(self, milestone, old_values):
        """
        `old_values` is a dictionary containing the previous values of the
        milestone properties that changed. Currently those properties can be
        'name', 'due', 'completed', or 'description'.
        """
        pass

    def milestone_deleted(self, milestone):
        """Called when a milestone is deleted."""
        pass
    
    #IRepositoryChangeListener methods
    def changeset_added(self, repos, changeset):
        """Called after a changeset has been added to a repository."""
        pass

    def changeset_modified(self, repos, changeset, old_changeset):
        """Called after a changeset has been modified in a repository.
       
        The `old_changeset` argument contains the metadata of the changeset
        prior to the modification. It is `None` if the old metadata cannot
        be retrieved.
        """
        pass
