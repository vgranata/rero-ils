# -*- coding: utf-8 -*-
#
# RERO ILS
# Copyright (C) 2021 RERO
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

"""Operation logs Record tests."""

from copy import deepcopy

import pytest
from invenio_search import current_search

from rero_ils.modules.operation_logs.api import OperationLog


def test_operation_create(client, es_clear, operation_log_data):
    """Test operation logs creation."""
    oplg = OperationLog.create(operation_log_data, index_refresh='wait_for')
    assert oplg
    assert oplg.id
    # need to compare with dumps as it has resolve $refs
    data = OperationLog.get_record(oplg.id)
    del data['_created']
    del data['_updated']
    assert data == OperationLog(operation_log_data).dumps()
    tmp = deepcopy(operation_log_data)
    tmp['date'] = '2020-01-21T09:51:52.879533+00:00'
    oplg2 = OperationLog.create(tmp, index_refresh='wait_for')
    assert OperationLog.get_indices() == set((
        'operation_logs-2020',
        'operation_logs-2021'
    ))
    assert OperationLog.get_record(oplg.id)
    assert OperationLog.get_record(oplg2.id)
    # clean up the index
    assert OperationLog.delete_indices()


def test_operation_bulk_index(client, es_clear, operation_log_data):
    """Test operation logs bulk creation."""
    data = []
    for date in [
        '2020-01-21T09:51:52.879533+00:00',
        '2020-02-21T09:51:52.879533+00:00',
        '2020-03-21T09:51:52.879533+00:00',
        '2020-04-21T09:51:52.879533+00:00',
        '2021-01-21T09:51:52.879533+00:00',
        '2021-02-21T09:51:52.879533+00:00'
    ]:
        tmp = deepcopy(operation_log_data)
        tmp['date'] = date
        data.append(tmp)
    OperationLog.bulk_index(data)
    # flush the index for the test
    current_search.flush_and_refresh(OperationLog.index_name)
    assert OperationLog.get_indices() == set((
        'operation_logs-2020',
        'operation_logs-2021'
    ))
    with pytest.raises(Exception) as excinfo:
        data[0]['operation'] = dict(name='foo')
        OperationLog.bulk_index(data)
        assert "BulkIndexError" in str(excinfo.value)
    # clean up the index
    assert OperationLog.delete_indices()


def test_update(app, es_clear, operation_log_data, monkeypatch):
    """Test update log."""
    operation_log = OperationLog.create(deepcopy(operation_log_data),
                                        index_refresh='wait_for')

    log_data = OperationLog.get_record(operation_log.id)
    assert log_data['record']['value'] == 'item4'

    # Update OK
    log_data['record']['value'] = '1234'
    OperationLog.update(log_data.id, log_data['date'], log_data)
    log_data = OperationLog.get_record(operation_log.id)
    assert log_data['record']['value'] == '1234'

    # Update KO
    monkeypatch.setattr(
        'elasticsearch_dsl.Document.update', lambda *args, **kwargs: 'error')
    with pytest.raises(Exception) as exception:
        OperationLog.update(log_data.id, log_data['date'], log_data)
        assert str(exception) == 'Operation log cannot be updated.'
