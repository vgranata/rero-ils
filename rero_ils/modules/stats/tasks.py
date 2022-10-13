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

"""Celery tasks for stats records."""

from celery import shared_task
from flask import current_app

from rero.ils.modules.stat.api import Stat, StatsForLibrarian,\
    StatsForPricing, StatsReport


@shared_task()
def collect_stats_billing():
    """Collect and store the statistics for billing."""
    stats_pricing = StatsForPricing().collect()
    with current_app.app_context():
        stat = Stat.create(
            dict(type='billing', values=stats_pricing),
            dbcommit=True, reindex=True)
        return f'New statistics of type {stat["type"]} has\
            been created with a pid of: {stat.pid}'


@shared_task()
def collect_stats_librarian():
    """Collect and store the montly statistics for librarian."""
    stats_librarian = StatsForLibrarian()
    date_range = {'from': stats_librarian.date_range['gte'],
                  'to': stats_librarian.date_range['lte']}
    stats_values = stats_librarian.collect()
    with current_app.app_context():
        stat = Stat.create(
            dict(type='librarian', date_range=date_range,
                 values=stats_values),
            dbcommit=True, reindex=True)
        return f'New statistics of type {stat["type"]} has\
            been created with a pid of: {stat.pid}'


@shared_task()
def collect_stats_report():
    """Collect and store the statistics for report."""

    cfg_pids = get_cfgs()
    for cfg_pid in cfg_pids:
        stats_report = StatsReport.create(cfg_pid)

    with current_app.app_context():
        stat = Stat.create(
            dict(type='report', values=stats_report),
            dbcommit=True, reindex=True)
        return f'New statistics of type {stat["type"]} has\
            been created with a pid of: {stat.pid}'

def get_cfgs():
    """Get pids of configurations for which a report has to be created today"""
    #TODO
    # check frequency of configurations
    # check date of today
    # return cfg pid if today is the first of the month and frequency is month
    # or if today is january first and frequency is year
    return