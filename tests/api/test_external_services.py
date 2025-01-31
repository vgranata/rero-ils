# -*- coding: utf-8 -*-
#
# RERO ILS
# Copyright (C) 2019 RERO
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

"""Tests REST API documents."""

# import json
# from utils import get_json, to_relative_url

import mock
from flask import url_for
from utils import VerifyRecordPermissionPatch, get_json, mock_response, \
    to_relative_url

from rero_ils.modules.documents.api import Document


@mock.patch('invenio_records_rest.views.verify_record_permission',
            mock.MagicMock(return_value=VerifyRecordPermissionPatch))
def test_documents_get(client, document):
    """Test record retrieval."""
    def clean_authorized_access_point(data):
        """Clean contribution from authorized_access_point_"""
        contributions = []
        for contribution in data.get('contribution', []):
            agent = {}
            for item in contribution['agent']:
                if item == 'authorized_access_point':
                    agent['preferred_name'] = contribution['agent'][item]
                elif not item.startswith('authorized_access_point_'):
                    agent[item] = contribution['agent'][item]
            contribution['agent'] = agent
            contributions.append(contribution)

        data.pop('sort_date_new', None)
        data.pop('sort_date_old', None)
        data.pop('sort_title', None)
        data.pop('isbn', None)
        return data

    item_url = url_for('invenio_records_rest.doc_item', pid_value='doc1')

    res = client.get(item_url)
    assert res.status_code == 200

    assert res.headers['ETag'] == '"{}"'.format(document.revision_id)

    data = get_json(res)
    assert document.dumps() == clean_authorized_access_point(data['metadata'])

    # Check self links
    res = client.get(to_relative_url(data['links']['self']))
    assert res.status_code == 200
    assert data == get_json(res)
    assert document.dumps() == clean_authorized_access_point(data['metadata'])

    list_url = url_for('invenio_records_rest.doc_list')
    res = client.get(list_url)
    assert res.status_code == 200
    data = get_json(res)
    data_clean = clean_authorized_access_point(
        data['hits']['hits'][0]['metadata']
    )
    document = document.replace_refs().dumps()
    assert document == data_clean

    list_url = url_for('invenio_records_rest.doc_list', q="Vincent Berthe")
    res = client.get(list_url)
    assert res.status_code == 200
    data = get_json(res)
    assert data['hits']['total']['value'] == 1


@mock.patch('requests.get')
@mock.patch('rero_ils.permissions.login_and_librarian',
            mock.MagicMock())
def test_documents_import_bnf_ean(mock_get, client, bnf_ean_any_123,
                                  bnf_ean_any_9782070541270,
                                  bnf_ean_any_9782072862014,
                                  bnf_anywhere_all_peter,
                                  bnf_recordid_all_FRBNF370903960000006):
    """Test document import from bnf."""

    mock_get.return_value = mock_response(
        content=bnf_ean_any_123
    )
    res = client.get(url_for(
        'api_imports.import_bnf',
        q='ean:any:123',
        no_cache=1
    ))
    assert res.status_code == 200
    data = get_json(res)
    assert not data.get('metadata')

    mock_get.return_value = mock_response(
        content=bnf_ean_any_9782070541270
    )
    res = client.get(url_for(
        'api_imports.import_bnf',
        q='ean:any:9782070541270',
        no_cache=1
    ))
    assert res.status_code == 200
    data = get_json(res).get('hits').get('hits')[0].get('metadata')
    assert data['pid'] == 'FRBNF370903960000006'
    assert Document.create(data)

    mock_get.return_value = mock_response(
        content=bnf_ean_any_9782072862014
    )
    res = client.get(url_for(
        'api_imports.import_bnf',
        q='ean:any:9782072862014',
        no_cache=1
    ))
    assert res.status_code == 200
    res_j = get_json(res)
    data = res_j.get('hits').get('hits')[0].get('metadata')
    data.update({
        "$schema": "https://bib.rero.ch/schemas/documents/document-v0.0.1.json"
    })
    assert Document.create(data)
    marc21_link = res_j.get('hits').get('hits')[0].get('links').get('marc21')

    res = client.get(marc21_link)
    data = get_json(res)
    assert data[0][0] == 'leader'

    res = client.get(url_for(
        'api_imports.import_bnf',
        q='',
        no_cache=1
    ))
    assert res.status_code == 200
    assert get_json(res) == {
        'aggregations': {},
        'hits': {
            'hits': [],
            'remote_total': 0,
            'total': 0
        }
    }

    mock_get.return_value = mock_response(
        content=bnf_anywhere_all_peter
    )
    res = client.get(url_for(
        'api_imports.import_bnf',
        q='peter',
        no_cache=1
    ))
    assert res.status_code == 200
    unfiltered_total = get_json(res)['hits']['remote_total']
    assert get_json(res)

    res = client.get(url_for(
        'api_imports.import_bnf',
        q='peter',
        year=2000,
        format='rerojson'
    ))
    assert res.status_code == 200
    assert get_json(res)['hits']['total'] < unfiltered_total

    res = client.get(url_for(
        'api_imports.import_bnf',
        q='peter',
        author='Peter Owen',
        format='rerojson'
    ))
    assert res.status_code == 200
    assert get_json(res)['hits']['total'] < unfiltered_total

    res = client.get(url_for(
        'api_imports.import_bnf',
        q='peter',
        document_type='docmaintype_book',
        document_subtype='docsubtype_other_book',
        format='rerojson'
    ))
    assert res.status_code == 200
    assert get_json(res)['hits']['total'] < unfiltered_total

    mock_get.return_value = mock_response(
        content=bnf_recordid_all_FRBNF370903960000006
    )
    res = client.get(url_for(
        'api_imports.import_bnf_record',
        id='FRBNF370903960000006',
        no_cache=1
    ))
    assert res.status_code == 200
    assert get_json(res).get('metadata', {}).get('identifiedBy')

    res = client.get(url_for(
        'api_imports.import_bnf_record',
        id='FRBNF370903960000006',
        format='rerojson'
    ))
    assert res.status_code == 200
    assert get_json(res).get('metadata', {}).get('ui_title_text')

    res = client.get(url_for(
        'api_imports.import_bnf_record',
        id='FRBNF370903960000006',
        format='marc'
    ))
    assert res.status_code == 200
    assert get_json(res)[1][1] == 'FRBNF370903960000006'
