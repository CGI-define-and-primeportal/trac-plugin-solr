from trac.core import Component, implements, TracError
from trac.web.chrome import add_stylesheet
from trac.ticket.api import ITicketChangeListener, IMilestoneChangeListener
from trac.wiki.api import IWikiChangeListener
from trac.attachment import IAttachmentChangeListener
from trac.versioncontrol.api import IRepositoryChangeListener
from trac.core import ExtensionPoint
from pkg_resources import resource_filename

class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener)

    # ITicketChangeListener methods
    def ticket_created(self, ticket):
        self.log.error("TICKET CREATED!")

    def ticket_changed(self, ticket, comment, author, old_values):
        self.log.error("TICKET CHANGED!")

    def ticket_deleted(ticket):
        self.log.error("TICKET DELETED!")
        
    #IWikiChangeListener methods
    def wiki_page_added(page):
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
