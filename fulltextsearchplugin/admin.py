import sys

from trac.admin import AdminCommandError, IAdminCommandProvider, PrefixList
from trac.core import Component, implements
from trac.util.translation import _
from trac.util.text import printout

from fulltextsearchplugin.fulltextsearch import FullTextSearch

class FullTextSearchAdmin(Component):
    """trac-admin command provider for full text search administration.
    """
    implements(IAdminCommandProvider)

    # IAdminCommandProvider methods

    def get_admin_commands(self):
        yield ('fulltext index', '[realm]',
               """Index Trac resources that are out of date
               
               When [realm] is specified, only that realm is updated.
               Synchronises the search index with Trac by indexing resources
               that have been added or updated.
               """,
               self._complete_admin_command, self._do_index)
        yield ('fulltext reindex', '[realm]',
               """Re-index all Trac resources.
               
               When [realm] is specified, only that realm is re-indexed.
               Discards the search index and recreates it. Note that this
               operation can take a long time to complete. If indexing gets
               interuppted, it can be resumed later using the `index` command.
               """,
               self._complete_admin_command, self._do_reindex)

    def _complete_admin_command(self, args):
        fts = FullTextSearch(self.env)
        if len(args) == 1:
            return PrefixList(fts.realms)

    def _index(self, realm, clean):
        fts = FullTextSearch(self.env)
        realms = [realm] or fts.realms
        if clean:
            printout(_("Wiping search index and re-indexing all items in "
                       "realms: %(realms)s", realms=fts._fmt_realms(realms)))
        else:
            printout(_("Indexing new and changed items in realms: %(realms)s",
                       realms=fts._fmt_realms(realms)))
        fts.index(realms, clean, self._index_feedback, self._clean_feedback)
        printout(_("Indexing finished"))

    def _index_feedback(self, realm, resource):
        #sys.stdout.write('\r\x1b[K %s' % (resource,))
        sys.stdout.flush()

    def _clean_feedback(self):
        #sys.stdout.write('\r\x1b[K')
        sys.stdout.flush()

    def _do_index(self, realm=None):
        self._index(realm, clean=False)

    def _do_reindex(self, realm=None):
        self._index(realm, clean=True)
