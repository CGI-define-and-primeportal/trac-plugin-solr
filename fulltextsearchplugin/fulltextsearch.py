from Queue import Queue
import os
from datetime import datetime
import re
import sunburnt
import types

from trac.env import IEnvironmentSetupParticipant
from trac.core import Component, implements, Interface, TracError
from trac.ticket.api import (ITicketChangeListener, IMilestoneChangeListener,
                             TicketSystem)
from trac.ticket.model import Ticket, Milestone
from trac.wiki.api import IWikiChangeListener, WikiSystem
from trac.wiki.model import WikiPage
from trac.util.text import shorten_line
from trac.attachment import IAttachmentChangeListener, Attachment
from trac.versioncontrol.api import IRepositoryChangeListener, Changeset
from trac.resource import (get_resource_shortname, get_resource_url,
                           Resource)
from trac.search import ISearchSource, shorten_result
from trac.util.translation import _
from trac.config import Option
from trac.util import datefmt
from trac.util.compat import partial
from trac.util.datefmt import to_utimestamp, utc

from componentdependencies import IRequireComponents
from tractags.model import TagModelProvider

from fulltextsearchplugin.dates import normalise_datetime

__all__ = ['IFullTextSearchSource',
           'FullTextSearchObject', 'Backend', 'FullTextSearch',
           ]

def _do_nothing(*args, **kwargs):
    pass

class IFullTextSearchSource(Interface):
    pass

class FullTextSearchModule(Component):
    pass

class FullTextSearchObject(object):
    def __init__(self, project, realm, id=None,
                 parent_realm=None, parent_id=None,
                 title=None, author=None, changed=None, created=None,
                 oneline=None, tags=None, involved=None,
                 popularity=None, body=None, comments=None, action=None,
                 **kwarg):
        # we can't just filter on the first part of id, because
        # wildcards are not supported by dismax in solr yet
        self.project = project
        if isinstance(realm, Resource):
            self.resource = realm
        else:
            parent = parent_realm and Resource(parent_realm, parent_id)
            self.resource = Resource(realm, id, parent=parent)

        self.title = title
        self.author = author
        self.changed = normalise_datetime(changed)
        self.created = normalise_datetime(created)
        self.oneline = oneline
        self.tags = tags
        self.involved = involved
        self.popularity = popularity
        self.body = body
        self.comments = comments
        self.action = action

    def _get_realm(self):
        return self.resource.realm
    def _set_realm(self, val):
        self.resource.realm = val
    realm = property(_get_realm, _set_realm)

    def _get_id(self):
        return self.resource.id
    def _set_id(self, val):
        self.resource.id = val
    id = property(_get_id, _set_id)

    @property
    def parent_realm(self):
        if self.resource.parent:
            return self.resource.parent.realm

    @property
    def parent_id(self):
        if self.resource.parent:
            return self.resource.parent.id

    @property
    def doc_id(self):
        if self.parent_realm and self.parent_id:
            return u"%s:%s:%s:%s:%s" % (self.project, self.realm,
                                        self.parent_realm, self.parent_id,
                                        self.id)
        else:
            return u"%s:%s:%s" % (self.project, self.realm, self.id)

    def __repr__(self):
        from pprint import pformat
        r = '<FullTextSearchObject %s>' % pformat(self.__dict__)
        return r


class Backend(Queue):
    """
    """

    def __init__(self, solr_endpoint, log, si_class=sunburnt.SolrInterface):
        Queue.__init__(self)
        self.log = log
        self.solr_endpoint = solr_endpoint
        self.si_class = si_class

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
        
    def remove(self, project_id, realms=None):
        s = self.si_class(self.solr_endpoint)
        realms = realms or []
        # I would have like some more info back
        s.delete(queries=[u"project:%s" % project_id] +
                         [u"realm:%s" % realm for realm in realms])
        s.commit()

    def commit(self):
        s = self.si_class(self.solr_endpoint)
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

    def optimize(self):
        s = self.si_class(self.solr_endpoint)
        try:
            s.optimize()
        except Exception:
            self.log.error('%s %r', item, item)
            raise


class FullTextSearch(Component):
    """Search all ChangeListeners and prepare the output for a full text 
       backend."""
    implements(ITicketChangeListener, IWikiChangeListener, 
               IAttachmentChangeListener, IMilestoneChangeListener,
               IRepositoryChangeListener, ISearchSource,
               IEnvironmentSetupParticipant, IRequireComponents)

    solr_endpoint = Option("search", "solr_endpoint",
                           default="http://localhost:8983/solr/",
                           doc="URL to use for HTTP REST calls to Solr")
    #Warning, sunburnt is case sensitive via lxml on xpath searches while solr is not
    #in the default schema fieldType and fieldtype mismatch gives problem
    def __init__(self):
        self.backend = Backend(self.solr_endpoint, self.log)
        self.project = os.path.split(self.env.path)[1]
        self._realms = [
            (u'ticket',     u'Tickets',     True,   self._reindex_ticket),
            (u'wiki',       u'Wiki',        True,   self._reindex_wiki),
            (u'milestone',  u'Milestones',  True,   self._reindex_milestone),
            (u'changeset',  u'Changesets',  True,   self._reindex_changeset),
            (u'source',     u'File archive', True,  None),
            (u'attachment', u'Attachments', True,   self._reindex_attachment),
            ]
        self._indexers = dict((name, indexer) for name, label, enabled, indexer
                                              in self._realms if indexer)

    @property
    def search_realms(self):
        return [name for name, label, enabled, indexer in self._realms]

    @property
    def index_realms(self):
        return [name for name, label, enabled, indexer in self._realms
                     if indexer]

    def _reindex(self, realm, resources, index_cb, feedback_cb, finish_cb):
        """Iterate through `resources` to index `realm`, return index count
        
        realm       Trac realm to which items in resources belong
        resources   Iterable of Trac resources e.g. WikiPage, Attachment
        index_cb    Callable that accepts a resource argument
        feedback_cb Callable that accepts a realm & resource argument
        finish_cb   Callable that accepts a realm & resource argument. The
                    resource will be None if `resources` is empty
        """
        i = -1
        resource = None
        for i, resource in enumerate(resources):
            index_cb(resource)
            feedback_cb(realm, resource)
        finish_cb(realm, resource)
        return i + 1

    def _reindex_changeset(self, realm, feedback, finish_fb):
        """Iterate all changesets and call self.changeset_added on them"""
        # TODO Multiple repository support
        repo = self.env.get_repository()
        def all_revs():
            rev = repo.oldest_rev
            yield rev
            while 1:
                rev = repo.next_rev(rev)
                if rev is None:
                    return
                yield rev
        resources = (repo.get_changeset(rev) for rev in all_revs())
        index = partial(self.changeset_added, repo)
        return self._reindex(realm, resources, index, feedback, finish_fb)

    def _reindex_wiki(self, realm, feedback, finish_fb):
        resources = (WikiPage(self.env, name)
                     for name in WikiSystem(self.env).get_pages())
        index = self.wiki_page_added
        return self._reindex(realm, resources, index, feedback, finish_fb)

    def _reindex_attachment(self, realm, feedback, finish_fb):
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
        def att(row):
            parent_realm, parent_id = row[0], row[1]
            attachment = Attachment(self.env, parent_realm, parent_id)
            attachment._from_database(*row[2:])
            return attachment
        resources = (att(row) for row in cursor)
        index = self.attachment_added
        return self._reindex(realm, resources, index, feedback, finish_fb)

    def _reindex_ticket(self, realm, feedback, finish_fb):
        db = self.env.get_read_db()
        cursor = db.cursor()
        cursor.execute("SELECT id FROM ticket")
        resources = (Ticket(tkt_id) for (tkt_id,) in cursor)
        index = self.ticket_created
        return self._reindex(realm, resources, index, feedback, finish_fb)

    def _reindex_milestone(self, realm, feedback, finish_fb):
        resources = Milestone.select(self.env)
        index = self.milestone_created
        return self._reindex(realm, resources, index, feedback, finish_fb)

    def _check_realms(self, realms):
        """Check specfied realms are supported by this component
        
        Raise exception if unsupported realms are found.
        """
        if realms is None:
            realms = self.index_realms
        unsupported_realms = set(realms).difference(set(self.index_realms))
        if unsupported_realms:
            raise TracError(_("These realms are not supported by "
                              "FullTextSearch: %(realms)s",
                              realms=self._fmt_realms(unsupported_realms)))
        return realms

    def _fmt_realms(self, realms):
        return ', '.join(realms)

    def remove_index(self, realms=None):
        realms = self._check_realms(realms)
        self.log.info("Removing realms from index: %s",
                      self._fmt_realms(realms))
        self.backend.remove(self.project, realms)

    def index(self, realms=None, clean=False, feedback=None, finish_fb=None):
        realms = self._check_realms(realms)
        feedback = feedback or _do_nothing
        finish_fb = finish_fb or _do_nothing

        if clean:
            self.remove_index(realms)
        self.log.info("Started indexing realms: %s",
                      self._fmt_realms(realms))
        summary = {}
        for realm in realms:
            indexer = self._indexers[realm]
            num_indexed = indexer(realm, feedback, finish_fb)
            self.log.debug('Indexed %i resources in realm: "%s"',
                           num_indexed, realm)
            summary[realm] = num_indexed
        self.log.info("Completed indexing realms: %s",
                      self._fmt_realms(realms))
        return summary

    def optimize(self):
        self.log.info("Started optimizing index")
        self.backend.optimize()
        self.log.info("Completed optimizing")

    # IRequireComponents methods
    def requires(self):
        return [TagModelProvider]

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
        self.index()
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
                body = u'%r' % (ticket.values,),
                comments = [t[4] for t in ticket.get_changelog()],
                )
        self.backend.create(so)
        self.log.debug("Ticket added for indexing: %s", ticket)
        
    def ticket_changed(self, ticket, comment, author, old_values):
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
                body = page.text,
                comments = [r[3] for r in history],
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
        so = FullTextSearchObject(self.project, page.resource.realm, old_name)
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
            # Prior to Trac 0.13 errors from a wrapped cursor are returned as
            # the native exceptions from the database library 
            # http://trac.edgewall.org/ticket/6348
            # sqlite3 raises OperationalError instead of ProgrammingError if
            # a queried table doesn't exist
            # http://bugs.python.org/issue7394
            # Following an error PostgresSQL requires that any transaction be
            # rolled back before further commands/queries are executes
            # psycopg2 raises InternalError to signal this
            # http://initd.org/psycopg/docs/faq.html
            if e.__class__.__name__ in ('ProgrammingError',
                                        'OperationalError'):
                db.rollback()
                return iter([])
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
                title = attachment.title,
                author = attachment.author,
                changed = attachment.date,
                created = created,
                body = attachment.open(),
                comments = comments,
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

    def _fill_so(self, changeset, node):
        so = FullTextSearchObject(
                self.project, node.resource,
                title = node.path,
                oneline = u'[%s]: %s' % (changeset.rev, shorten_result(changeset.message)),
                comments = [changeset.message],
                body = node.get_content(),
                changed = node.get_last_modified(),
                action = 'CREATE',
                author = changeset.author,
                created = changeset.date
                )
        return so

    #IRepositoryChangeListener methods
    def changeset_added(self, repos, changeset):
        """Called after a changeset has been added to a repository."""
        #Index the commit message
        so = FullTextSearchObject(
                self.project, changeset.resource,
                title=u'[%s]: %s' % (changeset.rev,
                                       shorten_line(changeset.message)),
                oneline=shorten_result(changeset.message),
                body=changeset.message,
                author=changeset.author,
                created=changeset.date,
                changed=changeset.date,
                )
        self.backend.create(so)

        # Index the file contents of the repository
        sos = []
        for path, kind, change, base_path, base_rev in changeset.get_changes():
            node = repos.get_node(path, changeset.rev)
            #FIXME handle kind == Node.DIRECTORY
            if change in (Changeset.ADD, Changeset.EDIT, Changeset.COPY):
                sos.append(self._fill_so(changeset, node))
            elif change == Changeset.MOVE:
                sos.append(FullTextSearchObject(self.project,
                                                node.resource.realm, base_path,
                                                action='DELETE'))
                sos.append(self._fill_so(changeset, node))
            elif change == Changeset.DELETE:
                sos.append(FullTextSearchObject(self.project,
                                                node.resource.realm, path,
                                                action='DELETE'))
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
        return [(name, label, enabled) for name, label, enabled, indexer 
                                       in self._realms]

    def get_search_results(self, req, terms, filters):
        self.log.debug("get_search_result called")
        result = self._do_search(terms, filters)
        docs = (FullTextSearchObject(**doc) for doc in result.result.docs)
        for doc in docs:
            changed = doc.changed
            href = get_resource_url(self.env, doc.resource, req.href)
            title = doc.title or get_resource_shortname(self.env, doc.resource)
            author = ", ".join(doc.author or [])
            excerpt = doc.oneline or ''
            yield (href, title, changed, author, excerpt)

    def _build_filter_query(self, si, filters):
        """Return a SOLR filter query that matches any of the chosen filters
        (realms).
        
        The filter is of the form realm:realm1 OR realm:realm2 OR ...
        """
        Q = si.query().Q
        my_filters = [f for f in filters if f in self.search_realms]
        def rec(list1):
            if len(list1) > 2:
                return Q(realm=list1.pop()) | rec(list1)
            elif len(list1) == 2:
                return Q(realm=list1.pop()) | Q(realm=list1.pop())
            elif len(list1) == 1:
                return Q(realm=list1.pop())
            else:
                # NB A TypeError will be raised if this string is combined
                #    with a LuceneQuery
                return ""
        return rec(my_filters[:])

    def _do_search(self, terms, filters):
        try:
            si = self.backend.si_class(self.solr_endpoint)
        except:
            raise

        # Restrict search to chosen realms, if none of our filters were chosen
        # then we won't have any results - return early, empty handed
        # NB Also avoids TypeError if _build_filter_query() returns a string
        filter_q = self._build_filter_query(si, filters)
        if not filter_q:
            return

        # The index can store multiple projects, restrict results to this one
        filter_q &= si.query().Q(project=self.project)

        if self._has_wildcard(terms):
            self.log.debug("Found wildcard query, switching to standard parser")
            result = si.query(terms).filter(filter_q).facet_by('realm').execute()
        else:
            opts = {
                # Search for terms (q) using dismax query type (qt).
                # No query fields (qf) are specified so search in default
                # fields, with default weightings
                'q': terms, 'qt': "dismax",
                # As well as the results, return num of results in each realm
                'facet': True, 'facet.field': "realm",
                # Filter query results by realm and project
                'fq': filter_q,
                }
            result = si.search(**opts)
        self.log.debug("Facets: %s", result.facet_counts.facet_fields)
        return result

    def _has_wildcard(self, terms):
        for term in terms:
            if '*' in term:
                return True
        return False
