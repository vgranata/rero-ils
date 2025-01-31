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

"""Click command-line interface for operation logs."""


from pprint import pprint

import arrow
import click
from flask.cli import with_appcontext

from rero_ils.modules.stats.api import Stat, StatsForPricing


@click.group()
def stats():
    """Notification management commands."""


@stats.command('dumps')
@with_appcontext
def dumps():
    """Dumps the current stats value."""
    pprint(StatsForPricing(to_date=arrow.utcnow()).collect(), indent=2)


@stats.command('collect')
@with_appcontext
def collect():
    """Extract the stats value and store it."""
    _stats = StatsForPricing(to_date=arrow.utcnow())
    stat = Stat.create(
        dict(values=_stats.collect()), dbcommit=True, reindex=True)
    click.secho(
        f'Stats collected and created. New pid: {stat.pid}', fg='green')
