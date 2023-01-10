#! /usr/bin/env python

import csv
import json
import calendar
import os
from pprint import pprint
import time

import click
from flask.cli import FlaskGroup, with_appcontext
from rero_ils.modules.cli.reroils import stats
from invenio_app.factory import create_app

from elasticsearch_dsl.query import Q
from datetime import datetime
from itertools import combinations

from invenio_search.api import RecordsSearch
from rero_ils.modules.documents.api import DocumentsSearch
from rero_ils.modules.holdings.api import HoldingsSearch
from rero_ils.modules.ill_requests.api import ILLRequestsSearch
from rero_ils.modules.items.api import ItemsSearch
from rero_ils.modules.items.models import ItemCirculationAction, TypeOfItem
from rero_ils.modules.libraries.api import LibrariesSearch
from rero_ils.modules.locations.api import Location
from rero_ils.modules.loans.logs.api import LoanOperationLog
from rero_ils.modules.organisations.api import Organisation
from rero_ils.modules.patrons.api import PatronsSearch, Patron
from rero_ils.modules.stats.api import StatsForLibrarian
from rero_ils.modules.utils import extracted_data_from_ref

path = 'rero_ils/stats_reports'
# path = '/network/nfs/data_ils/ils/stats'


class StatsReport(object):
    """Statistics for report."""
    # def number_of_combinations(self, n):
    #     fact_n = 1
    #     fact_n_2 = 1
    #     for i in range(1,n+1):
    #         fact_n = fact_n * i
    #     for i in range(1,n-1):
    #         fact_n_2 = fact_n_2 * i
    #     return fact_n/(2*fact_n_2)

    # def pid_simulator(self, pids):
    #     if len(pids) < self.limit_pids:
    #         pids.extend(pids)
    #         self.pid_simulator(pids)
    #     [pids.remove(pid) for pid in pids if len(pids) > self.limit_pids]
    #     return pids

    # def change_index_settings(self, index):
    #     from elasticsearch_dsl.connections import connections
    #     from elasticsearch_dsl import Index

    #     connections.create_connection(hosts=['127.0.0.1'])
    #     i=Index(index)
    #     print(i.get_settings())

    def __init__(self):
        self.indicators = ['number_of_checkouts', 'number_of_checkins',
                           'number_of_renewals', 'number_of_requests',
                           'number_of_documents',
                           'number_of_created_documents',
                           'number_of_items', 'number_of_created_items',
                           'number_of_deleted_items',
                           'number_of_holdings', 'number_of_created_holdings',
                           'number_of_patrons', 'number_of_ill_requests'
                           ]

        self.libraries = [{'pid': lib.pid, 'name': lib.name,
                           'org_pid': lib.organisation.pid}
                          for lib in StatsForLibrarian().get_all_libraries()]

        self.organisations = Organisation.get_all()
        self.locations = self.get_all_locations()

        self.trigger_mapping = \
            {'number_of_checkouts': ItemCirculationAction.CHECKOUT,
             'number_of_checkins': ItemCirculationAction.CHECKIN,
             'number_of_renewals': ItemCirculationAction.EXTEND,
             'number_of_requests': ItemCirculationAction.REQUEST}

        self.distributions_mapping = {
                        'number_of_documents':
                            {'library': 'holdings.organisation.library_pid',
                             'time_range': {'_created': 'month'}},
                        'number_of_items':
                            {'library': 'library.pid',
                             'location': 'location.pid',
                             'type': 'type',
                             'time_range': {'_created': 'month'}},
                        'number_of_circ_operations':
                            {'library': 'loan.item.library_pid',
                             'patron_type': 'loan.patron.type',
                             'document_type': 'loan.item.document.type',
                             'transaction_channel': 'loan.transaction_channel',
                             'pickup_location': 'loan.pickup_location.pid',
                             'time_range': {'date': 'month'}},
                              # TODO
                              # 'transaction_library': 'library.value', 
                              #  'item_location': 'loan.item.holding.location_name',
                        'number_of_created_documents':
                            {'library': 'library.value',
                             'time_range': {'date': 'month'}},
                        'number_of_holdings':
                            {'library': 'library.pid',
                             'holding_type': 'holdings_type',
                             'time_range': {'_created': 'month'}},
                        'number_of_ill_requests':
                                         {'library': 'library.pid',
                                         'status': 'status',
                                         'loan_status': 'loan_status',
                                         'time_range': {'_created': 'month'}},
                        'number_of_patrons':
                                         {'library': 'patron.libraries.pid',  # patron affiliation library
                                          'local_code': 'local_codes',
                                          'role': 'roles',
                                          'time_range': {'_created': 'month'}},
                        'number_of_deleted_items':
                                            {'library': 'library.value',
                                              'time_range': {'date': 'month'}},  # needs more than 1 query for distributions location and type
                        'number_of_created_items':
                                            {'library': 'library.value',
                                             'time_range': {'date': 'month'}},  # the library exists only for some items. Needs more than 1 query for distributions location and type
                        'number_of_created_holdings':
                                            {'library': 'library.value',
                                             'time_range': {'date': 'month'}},  # the library exists only for some items. Needs more than 1 query for distribution holding_type
                        'number_of_active_patrons': {},  # needs more than 1 query
                        'number_of_notifications': {},  # maybe not possible
                        }

        self.filter_indexes = {'operation_logs':
                               {'items': 'loan.item.pid',
                                'holdings': 'loan.holding.pid',
                                'patrons': 'loan.patron.pid',
                                'documents': 'loan.item.document.pid'}
                               }
        # TODO
        # 'documents': {'items': 'holdings.items.pid',
        #               'holdings': 'holdings.pid'}

        # 'items': {'documents': 'document.pid',
        #            'holdings': 'holding.pid'},

        # the default value is 65536
        # the maximum value that can be set is 2147483647
        self.limit_pids = 65536
        # self.limit_pids = 10

        self.config = {}

    
    def get_library_pids(self, librarian_pid):
        """Get libraries pids of the system librarian organisation."""

        library_pids = None # do not make report if not system librarian
        current_librarian = Patron.get_record_by_pid(librarian_pid)

        # if Patron.ROLE_LIBRARIAN in current_librarian["roles"]:
        #     library_pids = {
        #         extracted_data_from_ref(lib) for lib in
        #         current_librarian.get('libraries', [])}

        # case system_librarian: add libraries of organisation
        if Patron.ROLE_SYSTEM_LIBRARIAN in current_librarian["roles"]:
            patron_organisation = current_librarian.get_organisation()
            libraries_search = LibrariesSearch()\
                .filter('term', organisation__pid=patron_organisation.pid)\
                .source(['pid']).scan()
            library_pids = [s.pid for s in libraries_search]

        return library_pids


    def get_all_locations(self):
        """Get all locations.

        :returns: formatted location names
        """
        locations = []
        for pid in Location.get_all_pids():
            record = Location.get_record_by_pid(pid)
            locations.append({pid: f'{record["code"]}: {record["name"]}'})
        return locations

    def format_bucket_key(self, dist, bucket_dist):
        """ Format name of distribution to add in the results file

        :param dist: distribution name
        :param bucket_dist: bucket of the distribution
        :returns: formatted name of distribution
        """
        bucket_key_formatted = None
        if dist in ['location', 'pickup_location']:
            bucket_key_formatted = [v for loc in self.locations
                                    for k, v in loc.items()
                                    if k == bucket_dist.key]
            if bucket_key_formatted:
                bucket_key_formatted = bucket_key_formatted[0]
            else:
                click.secho(f'WARNING: location pid {bucket_dist.key}\
                            not in index locations', fg='yellow')

        if dist == 'time_range':
            date, _ = bucket_dist.key_as_string.split('T')
            date = datetime.strptime(date, '%Y-%m-%d')
            if self.config['period'] == 'year':
                bucket_key_formatted = f'{date.year}'
            else:
                bucket_key_formatted = f'{date.year}-{date.month}'

        return bucket_key_formatted or bucket_dist.key

    def query_results_1(self, res):
        """Process bucket results

        Library is in distribution 1
        :param res: result of query
        :returns: formatted results
        """
        indicator = self.config['indicator']
        dist2 = self.config['dist2']
        library_pids = self.config['library_pids']

        results = []
        for bucket in res.aggregations:
            for bucket_dist0 in bucket.buckets:
                library = [lib for lib in self.libraries
                            if lib['pid'] == bucket_dist0.key and
                            lib['pid'] in library_pids]
                if library:
                    for bucket_dist2 in bucket_dist0.dist2.buckets:
                        key_dist2 = self.format_bucket_key(dist2, bucket_dist2)
                        if bucket_dist2.doc_count:
                            results.append((key_dist2,
                                            library[0]['org_pid'],
                                            library[0]['pid'],
                                            library[0]['name'],
                                            bucket_dist2.doc_count))
        return results

    def query_results_2(self, res):
        """Process bucket results

        Library is not in distribution 1 or 2
        :param res: result of query
        :returns: formatted results
        """
        indicator = self.config['indicator']
        dist1 = self.config['dist1']
        dist2 = self.config['dist2']
        library_pids = self.config['library_pids']

        results = []
        for bucket in res.aggregations:
            for bucket_dist0 in bucket:
                library = [lib for lib in self.libraries
                            if lib['pid'] == bucket_dist0.key and
                            lib['pid'] in library_pids]
                for bucket_dist1 in bucket_dist0.dist1.buckets:
                    key_dist1 = self.format_bucket_key(dist1, bucket_dist1)
                    for bucket_dist2 in bucket_dist1.dist2.buckets:
                        key_dist2 = self.format_bucket_key(dist2, bucket_dist2)
                        if bucket_dist2.doc_count:
                            results.append((key_dist1,
                                            key_dist2,
                                            library[0]['org_pid'],
                                            library[0]['pid'],
                                            library[0]['name'],
                                            bucket_dist2.doc_count))
        return results

    def query_filter(self, query, main_index):
        """Add queries for filters to main query

        :param query: query on the main index
        :param main_index: the index of the indicator

        :return: updated library pids and query
        """
        filters = self.config['filters']
        library_pids = self.config['library_pids']

        query_index = None
        filter_pids = None

        if main_index in self.filter_indexes:
            filter_indexes = self.filter_indexes[main_index]

            for f in filters.items():
                filter_index = list(f)[0]
                if filter_index in filter_indexes and filter_index is not main_index:
                    if filter_index == 'items':
                        query_index = ItemsSearch()[0:0]
                        if library_pids:
                            query_index = query_index\
                                .filter('terms', library__pid=library_pids)
                    elif filter_index == 'documents':
                        query_index = DocumentsSearch()[0:0]
                        if library_pids:
                            query_index = query_index\
                                .filter(
                                    'terms',
                                    holdings__organisation__library_pid=library_pids)
                    elif filter_index == 'holdings':
                        query_index = HoldingsSearch()[0:0]
                        if library_pids:
                            query_index = query_index\
                                .filter('terms', library__pid=library_pids)
                    elif filter_index == 'patrons':
                        query_index = PatronsSearch()[0:0]
                        if library_pids:
                            query_index = query_index\
                                .filter('terms',
                                        patron__libraries__pid=library_pids)

                    filter = filters[filter_index]
                    results_filter = query_index\
                        .filter('bool', must=[Q('query_string',
                                                query=(filter))])\
                        .source(['pid'])\
                        .scan()
                    filter_pids = list(set([s.pid for s in results_filter]))
                    # IMPORTANT: for main indexes such as documents
                    # where the agg is on multiple libraries
                    # ('holdings.organisation.library_pid')
                    # the filter must include the library pid

                    # filter = filters[filter_index]
                    # results_filter = query_index\
                    #                 .filter('bool', must=[Q('query_string',
                    #                                       query=(filter))])\
                    #                 .source(['pid', 'library.pid'])\
                    #                 .scan()
                    # results = [(s.pid, s.library.pid)
                    #            for s in results_filter]
                    # filter_pids = list(set([s[0] for s in results]))

                    # results_library_pids = [s[1] for s in results]
                    # if library_pids:
                    #     library_pids = list(set(results_library_pids) &
                    #                   set(library_pids))
                    # else:
                    #     library_pids = list(set(results_library_pids))

                    # check the number of pids found is less than the limit
                    # otherwise abort report
                    if len(filter_pids) > self.limit_pids:
                        return None, None

                    query = query\
                        .filter('bool', must=[Q('terms',
                                **{filter_indexes[filter_index]:filter_pids})])

        if main_index in filters:
            query = query\
                    .filter('bool', must=[
                        Q('query_string', query=(filters[main_index]))])

        # pprint(query.to_dict())

        return library_pids, query

    def query_aggs(self, query, fields):
        """ Create aggregations and execute query

        :param query: indicator query
        :param fields: index fields on which to make the aggregations
        :returns: results of query
        """
        indicator = self.config['indicator']
        dist1 = self.config['dist1']
        dist2 = self.config['dist2']
        library_pids = self.config['library_pids']

        size = 10000
        field0 = fields['library']  # main filter
        field1 = fields[dist1]
        field2 = fields[dist2]

        if dist1 == 'library':
            if dist2 == 'time_range':
                field_time_range = list(fields['time_range'])
                value_time_range = fields['time_range'][field_time_range[0]]
                query.aggs\
                    .bucket('dist0', 'terms', field=field0, size=size)\
                    .bucket('dist2', 'date_histogram',
                            field=field_time_range[0],
                            calendar_interval=value_time_range)
            else:
                query.aggs\
                    .bucket('dist0', 'terms', field=field0, size=size)\
                    .bucket('dist2', 'terms', field=field2, size=size)
            # pprint(query.to_dict())
            res = query.execute()
            results = self.query_results_1(res)
            columns = (dist2, 'org_pid', 'library_pid', 'library_name',
                       indicator)
            results.insert(0, columns)
        else:
            if dist2 == 'time_range':
                field_time_range = list(fields['time_range'])
                value_time_range = fields['time_range'][field_time_range[0]]
                query.aggs\
                    .bucket('dist0', 'terms', field=field0, size=size)\
                    .bucket('dist1', 'terms', field=field1, size=size)\
                    .bucket('dist2', 'date_histogram',
                            field=field_time_range[0],
                            calendar_interval=value_time_range)
            else:
                query.aggs\
                    .bucket('dist0', 'terms', field=field0, size=size)\
                    .bucket('dist1', 'terms', field=field1, size=size)\
                    .bucket('dist2', 'terms', field=field2, size=size)
            res = query.execute()
            results = self.query_results_2(res)
            columns = (dist1, dist2, 'org_pid', 'library_pid', 'library_name',
                       indicator)
            results.insert(0, columns)

        return results

    # def read_config(config):
    #     """ Read report configuration """
    #     f = open(config)
    #     return json.load(f)

    def number_of(self, query, main_index, trigger=None):
        """Add filters and aggregations to query

        :param query: main index query
        :param main_index: main index
        :param trigger: trigger checkin, checkout, extend or request
        """
        indicator = self.config['indicator']
        dist1 = self.config['dist1']
        dist2 = self.config['dist2']
        filters = self.config['filters']
        library_pids = self.config['library_pids']

        if trigger:
            fields = self.distributions_mapping['number_of_circ_operations']
        else:
            fields = self.distributions_mapping[indicator]

        if library_pids:
            query = query\
                    .filter('bool', must=[
                            Q('terms', **{fields['library']:library_pids})])

        # add filter query
        if filters:
            library_pids, query = self.query_filter(query, main_index)
            self.config['library_pids'] = library_pids
            if not query:
                return

        # swap distributions, always put time_range in dist2
        if 'time_range' == dist1:
            dist1 = dist2
            dist2 = 'time_range'
            self.config['dist1'] = dist1
            self.config['dist2'] = dist2
        
        # make aggregations according to distributions and execute query
        results = self.query_aggs(query, fields)
        # pprint(query.to_dict())
        return results

    def make_report(self, data):
        """Make report

        :param data: report configuration data
        # https://github.com/elastic/elasticsearch-dsl-py/issues/610
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-extended-stats-bucket-aggregation.html
        # https://discuss.elastic.co/t/sql-like-group-by-and-having/104705
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html

        # https://stackoverflow.com/questions/54520671/setting-index-max-terms-count-has-no-effect
        """
        # category = data.get('category')
        indicator = data.get('indicator')
        dist1 = data.get('distribution1')
        dist2 = data.get('distribution2')
        filters = data.get('filters')
        library_pids = data.get('lib_pids')
        librarian_pid = data.get('librarian_pid')
        org_pid = data.get('org_pid')
        period = data.get('period')
        
        trigger = None
        results = []

        if library_pids:
            library_pids = list(set(library_pids.split(',')))
            # print(f"library pids: {library_pids}")

        if librarian_pid:
            librarian_library_pids =\
                self.get_library_pids(librarian_pid)
            # print(f"librarian_library_pids: {librarian_library_pids}")
            if library_pids:
                library_pids = list(set(library_pids) & 
                                    set(librarian_library_pids))
                if not library_pids:
                    print(f"WARNING: report not done because {data.get('lib_pids')} is not a library of the system librarian")
                    return
            else:
                library_pids = librarian_library_pids
            # print(f"final library pids: {library_pids}")
                if not library_pids:
                    print(f"WARNING: report not done because the librarian {data.get('librarian_pid')} has no libraries")
                    return

        # case admin
        if not (librarian_pid and library_pids):
            library_pids = [lib['pid'] for lib in self.libraries]

        if org_pid:
            library_pids = [lib['pid'] for lib in self.libraries
                            if lib['org_pid']==org_pid]

        self.config = {'indicator': indicator,
                       'dist1': dist1,
                       'dist2': dist2,
                       'filters': filters,
                       'library_pids': library_pids,
                       'librarian_pid': librarian_pid,
                       'period': period}

        if indicator in list(self.trigger_mapping):
            trigger = self.trigger_mapping[indicator]
            dists = list(
                    self.distributions_mapping['number_of_circ_operations'])
        else:
            dists = list(self.distributions_mapping[indicator])

        if not (dist1 and dist2):
            dists_pairs = list(combinations(dists, 2))
            # assert len(dists_pairs) == \
            # self.number_of_combinations(len(dists))
        else:
            dists_pairs = [(dist1, dist2)]

        # change time_range calendar interval
        if period == 'year':
            if indicator in list(self.trigger_mapping):
                indicator_key = 'number_of_circ_operations'
            else:
                indicator_key = indicator
            time_range =\
                self.distributions_mapping[indicator_key]['time_range']
            time_range[next(iter(time_range.keys()))] = period
            self.distributions_mapping[indicator_key]['time_range'] =\
                time_range

        print(f'Number of files to create: {len(dists_pairs)}')

        for dist in dists_pairs:
            print(f'Processing {indicator} - {dist[0]} vs {dist[1]} \
                  - filters: {filters}')
            filename = f'{indicator}_{dist[0]}_vs_{dist[1]}'
            self.config['dist1'] = dist[0]
            self.config['dist2'] = dist[1]

            if indicator in list(self.trigger_mapping):
                query = RecordsSearch(index=LoanOperationLog.index_name)[0:0]\
                        .filter('term', record__type='loan')\
                        .filter('term', loan__trigger=trigger)
                results = self.number_of(query, 'operation_logs', trigger)
            elif indicator == 'number_of_created_documents':
                query = RecordsSearch(index=LoanOperationLog.index_name)[0:0]\
                        .filter('term', record__type='doc')\
                        .filter('term', operation='create')
                results = self.number_of(query, 'operation_logs')
            elif indicator == 'number_of_documents':
                query = DocumentsSearch()[0:0]
                results = self.number_of(query, 'documents')
            elif indicator == 'number_of_items':
                query = ItemsSearch()[0:0]
                results = self.number_of(query, 'items')
            elif indicator == 'number_of_holdings':
                query = HoldingsSearch()[0:0]
                results = self.number_of(query, 'holdings')
            elif indicator == 'number_of_ill_requests':
                query = ILLRequestsSearch()[0:0]
                results = self.number_of(query, 'ill_requests')
            elif indicator == 'number_of_deleted_items':
                query = RecordsSearch(index=LoanOperationLog.index_name)[0:0]\
                        .filter('term', record__type='item')\
                        .filter('term', operation='delete')
                results = self.number_of(query, 'operation_logs')
            elif indicator == 'number_of_created_items':
                query = RecordsSearch(index=LoanOperationLog.index_name)[0:0]\
                        .filter('term', record__type='item')\
                        .filter('term', operation='create')
                results = self.number_of(query, 'operation_logs')
            elif indicator == 'number_of_created_holdings':
                query = RecordsSearch(index=LoanOperationLog.index_name)[0:0]\
                        .filter('term', record__type='hold')\
                        .filter('term', operation='create')
                #         .source(['date','record.value'])\
                #         .scan()
                # for s in query:
                #     print(s.date)
                results = self.number_of(query, 'operation_logs')
            elif indicator == 'number_of_patrons':
                query = PatronsSearch()[0:0]
                results = self.number_of(query, 'patrons')
            elif indicator == 'number_of_active_patrons':
                pass
            elif indicator == 'number_of_notifications':
                pass
            
            if results:
                # make csv file
                if len(results) > 1:
                    if self.config['dist1'] == 'library':
                        results = self.table_one_distribution(results)
                    else:
                        results = self.table_two_distributions(results)

                # add infos on filter to results
                results[0] = results[0] + ("",) + \
                             (f'filters = {data["filters"]}',)

                self.make_csv(results, filename, indicator)
            else:
                click.secho(f'ABORTED: The report could not be created.',
                            fg='red')

    def make_folder(self, path):
        """Create folder

        :param: path of folder
        """
        if not os.path.exists(path):
            os.makedirs(path)

    def make_csv(self, data, filename, indicator):
        """Make csv file

        :param data: query results
        :param filename: name of file to create
        :param indicator: indicator
        """
        file_path = f'{path}/results/{indicator}'
        self.make_folder(file_path)
        filename = f'{filename}.csv'

        with open(f'{file_path}/{filename}', 'w') as f:
            writer = csv.writer(f)
            for d in data:
                writer.writerow(d)

    def table_one_distribution(self, data):
        """Create table with distribution on x axis

        The distribution is on the x axis and the library
        is on the y axis.
        :param data: query results
        :returns: formatted data table
        """
        # unique values of distribution 1 as header
        header = list(set(list(zip(*data[1:]))[0]))
        len_header = len(header)
        for value in ['library_name',
                        'library_pid', 'org_pid']:
            header.insert(0, value)

        processed_data = []
        for d in data[1:]:
            row = [d[1], d[2], d[3]]
            # prefill with value 0
            for i in range(len_header):
                row.insert(3+i, 0)
            processed_data.append(row)
        processed_data = [list(x) for x in set(tuple(x)
                          for x in processed_data)]
        processed_data = sorted(processed_data, key=lambda x: x[1])

        for d2 in processed_data:
            d2_index = processed_data.index(d2)
            for d1 in data[1:]:
                # library
                if d2[1] == d1[2]:
                    # distribution 1 value
                    index = header.index(d1[0])
                    # score
                    d2[index] = d1[4]
                    processed_data[d2_index] = d2

        processed_data.insert(0, tuple(header))
        return processed_data


    def table_two_distributions(self, data):
        """Create table with distributions on x axis and y axis

        Distribution 1 is on the x axis and distribution 2 and the library
        are on the y axis.
        :param data: query results
        :returns: formatted data table
        """
        # unique values of distribution 1 as header
        header = list(set(list(zip(*data[1:]))[0]))
        len_header = len(header)

        if self.config['dist2'] == 'time_range':
            if self.indicator in ['number_of_checkouts', 'number_of_checkins',
                                  'number_of_renewals', 'number_of_requests']:
                name_dist2 = 'transaction_date'
            elif self.indicator in ['number_of_documents',
                                    'number_of_created_documents',
                                    'number_of_items',
                                    'number_of_created_items',
                                    'number_of_holdings',
                                    'number_of_created_holdings',
                                    'number_of_patrons',
                                    'number_of_ill_requests']:
                name_dist2 = 'creation_date'
            elif self.indicator == 'number_of_deleted_items':
                name_dist2 = 'deletion_date'
        else:
            name_dist2 = self.config['dist2']

        for value in [name_dist2, 'library_name',
                        'library_pid', 'org_pid']:
            header.insert(0, value)
        
        processed_data = []
        for d in data[1:]:
            row = [d[2], d[3], d[4], d[1]]
            # prefill with value 0
            for i in range(len_header):
                row.insert(4+i, 0)
            processed_data.append(row)
        processed_data = [list(x) for x in set(tuple(x)
                          for x in processed_data)]
        processed_data = sorted(processed_data, key=lambda x: x[1])

        for d2 in processed_data:
            d2_index = processed_data.index(d2)
            for d1 in data[1:]:
                # library
                if d2[1] == d1[3]:
                    # distribution 2 value
                    if d2[3] == d1[1]:
                        # distribution 1 value
                        index = header.index(d1[0])
                        # score
                        d2[index] = d1[5]
                        processed_data[d2_index] = d2

        processed_data.insert(0, tuple(header))
        return processed_data

@click.group(cls=FlaskGroup, create_app=create_app)
def app_cli():
    """All app commands."""
    pass


@stats.command('report')
@click.argument('indicator', type=str)
@click.option('-librarian_pid', default=None,
              help='librarian or system_librarian pid')
@click.option('-lib_pids', default=None,
              help='library pids separated by comma. Ex: 1,2,3')
@click.option('-org_pid', default=None,
              help='organisation pid')
@click.option('-d1', default=None, help='distribution 1')
@click.option('-d2', default=None, help='distribution 2')
@click.option('-f1_index', default=None, help='index for filter 1')
@click.option('-f1', default=None, help='filter 1')
@click.option('-f2_index', default=None, help='index for filter 2')
@click.option('-f2', default=None, help='filter 2')
@click.option('-period', type=click.Choice(['month', 'year']),
              default='month',
              help='time range period, default is month,\
                    it can be set to year')
@with_appcontext
def report(indicator,
           librarian_pid, lib_pids, org_pid,
           d1, d2,
           f1_index, f1, f2_index, f2, period):
    """Make stats report.

    :param indicator: indicator of the statistics
    :param librarian_pid: librarian or system_librarian pid
    :param lib_pids: library pids separated by comma. Ex: 1,2,3
    :param org_pid: organisation pid
    :param d1: distribution 1
    :param d2: distribution 2
    :param f1_index: filter 1 index
    :param f1: filter 1
    :param f2_index: filter 2 index
    :param f2: filter 2
    :param period: time range for statistics, can be 'month' or 'year'
    """
    start = time.time()
    filters = {}
    if f1:
        filters[f1_index] = f1
    if f2:
        filters[f2_index] = f2

    data = {'indicator': indicator,
              'distribution1': d1,
              'distribution2': d2,
              'filters': filters,
              'lib_pids': lib_pids,
              'org_pid': org_pid,
              'librarian_pid': librarian_pid,
              'period': period}

    report = StatsReport()

    if indicator not in report.indicators:
        click.secho(f'{indicator} is not a valid indicator.\
                    Possible indicators are {report.indicators}', fg='red')
        return

    if lib_pids:
        library_pids = lib_pids.split(',')
        try:
            [int(pid) for pid in library_pids]
        except ValueError:
            click.secho(f'ERROR: Library pids should be integers', fg='red')
            return

    if indicator in list(report.trigger_mapping):
        dists = list(report.distributions_mapping['number_of_circ_operations'])
    else:
        dists = list(report.distributions_mapping[indicator])

    if d1 and not d2:
        click.secho(f'ERROR: missing distribution2.\
                    Possible distributions are {dists}', fg='red')
        return

    if d2 and not d1:
        click.secho(f'ERROR: missing distribution1.\
                    Possible distributions are {dists}', fg='red')
        return

    if (d1 and d1 not in dists):
        click.secho(f'ERROR: distribution1 is not valid.\
                    Possible distributions are {dists}', fg='red')
        return
    if (d2 and d2 not in dists):
        click.secho(f'ERROR: distribution2 is not valid.\
                    Possible distributions are {dists}', fg='red')
        return

    if f1 and not f1_index:
        click.secho(f'ERROR: missing index of filter 1.', fg='red')
        return
    if f2 and not f2_index:
        click.secho(f'ERROR: missing index of filter 2.', fg='red')
        return
    if f1_index and not f1:
        click.secho(f'ERROR: missing filter for filter index 1.', fg='red')
        return
    if f2_index and not f2:
        click.secho(f'ERROR: missing filter for filter index 2.', fg='red')
        return

    report.make_report(data)
    end = time.time()
    print(end-start)


# make this file usable as script
if __name__ == "__main__":
    app_cli()
