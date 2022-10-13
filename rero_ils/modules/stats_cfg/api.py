# -*- coding: utf-8 -*-
#
# RERO ILS
# Copyright (C) 2022 RERO
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Statistics configuration for report."""
from functools import partial

from rero_ils.modules.stats_cfg.models import StatCfgIdentifier, StatCfgMetadata
from rero_ils.modules.api import IlsRecord, IlsRecordsIndexer, IlsRecordsSearch
from rero_ils.modules.fetchers import id_fetcher
from rero_ils.modules.minters import id_minter
from rero_ils.modules.patrons.api import Patron, current_librarian
from rero_ils.modules.providers import Provider
# from rero_ils.modules.stats.api import StatsSearch
from rero_ils.modules.stats_cfg.extensions import StatConfigDataExtension

# provider
StatCfgProvider = type(
    'StatCfgProvider',
    (Provider,),
    dict(identifier=StatCfgIdentifier, pid_type='stacfg')
)
# minter
stat_cfg_id_minter = partial(id_minter, provider=StatCfgProvider)
# fetcher
stat_cfg_id_fetcher = partial(id_fetcher, provider=StatCfgProvider)


class StatsCfgSearch(IlsRecordsSearch):
    """Statistics configuration search."""

    class Meta:
        """Search only on stats_cfg index."""

        index = 'stats_cfg'
        doc_types = None
        fields = ('*',)
        facets = {}

        default_filter = None


class Stat_cfg(IlsRecord):
    """Statistics configuration class."""
    minter = stat_cfg_id_minter
    fetcher = stat_cfg_id_fetcher
    provider = StatCfgProvider
    model_cls = StatCfgMetadata

    # _extensions = [
    #     StatConfigDataExtension()
    # ]

    # def __init__(self, data, model=None, **kwargs):
    #     """Initialize instance with dictionary data and SQLAlchemy model.
    #     :param data: Dict with record metadata.
    #     :param model: StatCfgMetadata instance.
    #     """
    #     # self.model = self.model_cls
    #     for e in self._extensions:
    #         e.pre_init(self, data, model)
    #     super().__init__(data or {}, model=self.model_cls)


    @classmethod
    def create(cls, data, id_=None, delete_pid=False,
               dbcommit=False, reindex=False, **kwargs):
        """Create report configuration.
        
        :param data: data of report configuration
        """
        if 'librarian_pid' not in data:
            librarian = current_librarian
        else:
            librarian = Patron.get_record_by_pid(data['librarian_pid'])
        
        if not librarian.is_system_librarian:
            return

        # TODO Use extensions        
        categories = {
            'number_of_checkouts': 'Circulation', 
            'number_of_checkins': 'Circulation',
            'number_of_renewals': 'Circulation',
            'number_of_requests': 'Circulation',
            'number_of_documents': 'Catalogue',
            'number_of_created_documents': 'Catalogue',
            'number_of_items': 'Catalogue',
            'number_of_created_items': 'Catalogue',
            'number_of_deleted_items': 'Catalogue',
            'number_of_holdings': 'Catalogue',
            'number_of_created_holdings': 'Catalogue',
            'number_of_patrons': 'User management',
            'number_of_active_patrons': 'User management',
            'number_of_ill_requests': 'Circulation',
            'number_of_notifications': 'Administration'
        }

        data['org_pid'] = librarian.get_organisation().get('pid')
        data['category'] = categories[data['indicator']]
        if not data['period']:
            data['period'] = 'month'
        if not data['frequency']:
            data['frequency'] = 'month'

        return super().create(data, dbcommit=True, reindex=True)


    def update(self, data):
        """Update data for record.
        
        indicator, dist1, dist2, filters, org_pid, category 
        cannot be changed if there is a report for the configuration.
        """
        # do not update if there are reports for this configuration
        if self.get_reports():
            return

        # do not update if any of these fields have been changed
        fields = ['indicator', 'dist1', 'dist2',
                  'filters', 'org_pid', 'category']
        record = self.get_record_by_pid(self.pid)
        for field in fields:
            if not record[field] == data[field]:
                return

        super().update(data, commit=True, dbcommit=True, reindex=True)
        return self

    @classmethod
    def get_cfgs(cls, librarian_pid):
        """Get report configurations for system librarian.

        System librarian can see all reports configurations of the
        organisation.
        :param librarian_pid: system librarian pid
        :returns: list of configuration pids
        """
        librarian = Patron.get_record_by_pid(librarian_pid)
        if Patron.ROLE_SYSTEM_LIBRARIAN in librarian["roles"]:
            org_pid = \
                librarian.get_organisation().get('pid')
        else:
            return
        search = StatsCfgSearch()\
            .filter("term", org_pid=org_pid)\
            .scan()
        
        return [s for s in search]

    @classmethod
    def get_links_to_me(cls, pid, get_pids=False):
        """Record links.

        :param pid: report configuration pid
        :param get_pids: if True list of linked pids
                         if False count of linked reports
        :return: dict with number of reports or reports pids
        """
        links = {}

        # search for reports of configuration
        search = StatsSearch()\
            .filter("term", type='report')\
            .filter("term", config_pid=pid)

        if get_pids:
            search = search.source(['pid']).scan()
            reports = [s.pid for s in search]
        else:
            reports = search.count()

        # get number of reports or list of reports pids for configuration
        if reports:
            links['reports'] = reports
        return links

    def reasons_not_to_delete(self):
        """Get reasons not to delete report config.
        
        :param pid: report configuration pid
        :return: dict with number of reports or reports pids
        """
        cannot_delete = {}
        # Note: not possible to delete configuration 
        # if there are reports for the configuration.
        links = cls.get_links_to_me(self.pid)
        if links:
            cannot_delete['links'] = links
        return cannot_delete

    def get_reports(self):
        """Get reasons not to delete report config.
        
        :return: list of reports pids
        """
        from rero_ils.modules.stats.api import StatsSearch
        search = StatsSearch()\
            .filter("term", type='report')\
            .filter("term", config_pid=self.pid)\
            .source(['pid']).scan()
        return [s for s in search]


class StatsCfgIndexer(IlsRecordsIndexer):
    """Indexing stats configuration in Elasticsearch."""

    record_cls = Stat_cfg

    def bulk_index(self, record_id_iterator):
        """Bulk index records.

        :param record_id_iterator: Iterator yielding record UUIDs.
        """
        super().bulk_index(record_id_iterator, doc_type='stacfg')
