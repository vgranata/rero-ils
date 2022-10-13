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

"""Stats configuration record extensions."""

from invenio_records.extensions import RecordExtension

from rero_ils.modules.patrons.api import Patron, current_librarian

# TODO
class StatConfigDataExtension(RecordExtension):
    """Add related stats configuration data extension."""
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

    def pre_init(self, record, data, model, **kwargs):
    # def pre_commit(self, data):
        """Add stats configuration data.

        :param data: the record metadata.
        """
        if 'librarian_pid' not in data:
            librarian = current_librarian
        else:
            librarian = Patron.get_record_by_pid(data['librarian_pid'])
        
        data['org_pid'] = librarian.get_organisation().get('pid')
        data['category'] = self.categories[data['indicator']]
        if not data['period']:
            data['period'] = 'month'
        if not data['frequency']:
            data['frequency'] = 'month'

