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

from trac.test import Mock, MockPerm
from trac.web.href import Href
from trac.wiki.formatter import HtmlFormatter
from trac.mimeview import Context
from trac.core import Interface

class FullTextSearchObject(dict):
    possible_fields = ('id', 'author', 'changed', 'created', 'oneline', 'type', 
                       'tags', 'involved', 'body', 'popularity')

    def __init__(self, id, **kwargs):
        self.id = id
        self.__dict__.update(kwargs)

class IFullTextSearchProvider(Interface):
    """Extension point interface for components providing full text search 
       objects. Html is permitted."""

    def get_fulltext():
        """Return a FullTextSearchObject."""


class Backend(Queue):
    """
    """
    def create(self, id, item):
        self.put(('CREATE', id, item))
        
    def modify(self, id, item):
        self.put(('MODIFY', id, item))
    
    def delete(self, id, item):
        self.put(('DELETE', id, item))
        
class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener)
    
    def __init__(self):
        self.backend = Backend()

##    def render_wiki_to_html_without_req(self, thing, wikitext):
##        if wikitext is None:
##            return ""
##        try:
##            req = Mock(
##                href=Href(self.env.abs_href()),
##                abs_href=self.env.abs_href,
##                authname='anonymous', 
##                perm=MockPerm(),
##                chrome=dict(
##                    warnings=[],
##                    notices=[]
##                    ),
##                args={}
##            )
##            context = Context.from_request(req, thing.resource.realm, thing.resource.id)
##            formatter = HtmlFormatter(self.env, context, wikitext)
##            return formatter.generate(True)
##        except Exception, e:
##            raise
##            self.log.error("Failed to render %s", repr(wikitext))
##            self.log.error(exception_to_unicode(e, traceback=True))
##            return wikitext    
        
    # ITicketChangeListener methods
    def ticket_created(self, ticket):
        project_id = os.path.split(self.env.path)[1]
        unique_id = u"%s.%s.%s"%(project_id,ticket.resource.realm,ticket.resource.id)
        text = None
#        text = self.render_wiki_to_html_without_req(ticket, ticket['description'])
        text = ticket.resource.get_fulltext()
        self.backend.create(unique_id, text)
        self.log.debug("Ticket added for indexing: %s, %s"%(unique_id, text))

    def ticket_changed(self, ticket, comment, author, old_values):
        project_id = os.path.split(self.env.path)[1]
        unique_id = u"%s.%s.%s"%(project_id,ticket.resource.realm,ticket.resource.id)
        self.backend.modify(unique_id, ticket)
        self.log.error("TICKET CHANGED!")

    def ticket_deleted(ticket):
        project_id = os.path.split(self.env.path)[1]
        unique_id = u"%s.%s.%s"%(project_id,ticket.resource.realm,ticket.resource.id)
        self.backend.delete(unique_id, ticket)
        self.log.error("TICKET DELETED!")
        
    #IWikiChangeListener methods
    def wiki_page_added(page):
        project_id = os.path.split(self.env.path)[1]
        unique_id = u"%s.%s.%s"%(project_id,page.resource.realm,page.resource.id)
        context = Context.from_request(req, ticket.resource)
        out = format_to_html(self.env, context, ticket[name],
                       escape_newlines=self.must_preserve_newlines)
        self.backend.create(unique_id, out)
        pass

    def wiki_page_changed(page, version, t, comment, author, ipnr):
        pass

    def wiki_page_deleted(page):
        pass

    def wiki_page_version_deleted(page):
        pass

    def wiki_page_renamed(page, old_name): 
        pass

    #IAttachmentChangeListener methods
    def attachment_added(attachment):
        """Called when an attachment is added."""
        pass

    def attachment_deleted(attachment):
        """Called when an attachment is deleted."""
        pass

    def attachment_reparented(attachment, old_parent_realm, old_parent_id):
        """Called when an attachment is reparented."""
        pass
    
    #IMilestoneChangeListener methods
    def milestone_created(milestone):
        pass

    def milestone_changed(milestone, old_values):
        """
        `old_values` is a dictionary containing the previous values of the
        milestone properties that changed. Currently those properties can be
        'name', 'due', 'completed', or 'description'.
        """
        pass

    def milestone_deleted(milestone):
        """Called when a milestone is deleted."""
        pass
    
    #IRepositoryChangeListener methods
    def changeset_added(repos, changeset):
        """Called after a changeset has been added to a repository."""
        pass

    def changeset_modified(repos, changeset, old_changeset):
        """Called after a changeset has been modified in a repository.
       
        The `old_changeset` argument contains the metadata of the changeset
        prior to the modification. It is `None` if the old metadata cannot
        be retrieved.
        """
        pass
