# -*- coding: utf-8 -*-
#
# RERO ILS
# Copyright (C) 2019 RERO
# Copyright (C) 2020 UCLouvain
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

"""Click command-line utilities."""

from __future__ import absolute_import, print_function

import difflib
import itertools
import json
import logging
import multiprocessing
import os
import re
import sys
import traceback
from collections import OrderedDict
from datetime import datetime
from glob import glob
from pprint import pprint
from time import sleep
from uuid import uuid4

import click
import dateparser
import yaml
from celery import current_app as current_celery
from dojson.contrib.marc21.utils import create_record
from elasticsearch_dsl.query import Q
from flask import current_app
from flask.cli import with_appcontext
from flask_security.confirmable import confirm_user
from invenio_accounts.cli import commit, users
from invenio_db import db
from invenio_jsonschemas.proxies import current_jsonschemas
from invenio_oauth2server.cli import process_scopes, process_user
from invenio_oauth2server.models import Client, Token
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_records.api import Record
from invenio_records.models import RecordMetadata
from invenio_records_rest.utils import obj_or_import_string
from invenio_search.cli import es_version_check
from invenio_search.proxies import current_search, current_search_client
from jsonpatch import make_patch
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from lxml import etree
from werkzeug.local import LocalProxy
from werkzeug.security import gen_salt

from rero_ils.modules.locations.api import Location

from .api import IlsRecordsIndexer
from .collections.cli import create_collections
from .contributions.api import Contribution
from .contributions.tasks import create_mef_record_online
from .documents.api import Document, DocumentsIndexer, DocumentsSearch
from .documents.dojson.contrib.marc21tojson import marc21
from .documents.views import get_cover_art
from .holdings.cli import create_patterns
from .ill_requests.cli import create_ill_requests
from .items.api import Item
from .items.cli import create_items, reindex_items
from .libraries.api import Library
from .loans.cli import create_loans, load_virtua_transactions
from .monitoring import Monitoring
from .operation_logs.cli import create_operation_logs, \
    destroy_operation_logs, dump_operation_logs
from .patrons.cli import import_users, users_validate
from .selfcheck.cli import create_terminal, list_terminal, update_terminal
from .tasks import process_bulk_queue
from .utils import JsonWriter, bulk_load_metadata, bulk_load_pids, \
    bulk_load_pidstore, bulk_save_metadata, bulk_save_pids, \
    bulk_save_pidstore, csv_metadata_line, csv_pidstore_line, \
    extracted_data_from_ref, get_record_class_from_schema_or_pid_type, \
    number_records_in_file, read_json_record, read_xml_record
from ..modules.providers import append_fixtures_new_identifiers
from ..modules.utils import get_schema_for_resource

_datastore = LocalProxy(lambda: current_app.extensions['security'].datastore)
_records_state = LocalProxy(lambda: current_app.extensions['invenio-records'])


def abort_if_false(ctx, param, value):
    """Abort command is value is False."""
    if not value:
        ctx.abort()


@click.group()
def fixtures():
    """Fixtures management commands."""


fixtures.add_command(import_users)
fixtures.add_command(create_items)
fixtures.add_command(reindex_items)
fixtures.add_command(create_loans)
fixtures.add_command(load_virtua_transactions)
fixtures.add_command(create_patterns)
fixtures.add_command(create_ill_requests)
fixtures.add_command(create_collections)
fixtures.add_command(create_operation_logs)
fixtures.add_command(dump_operation_logs)
fixtures.add_command(destroy_operation_logs)


@users.command('confirm')
@click.argument('user')
@with_appcontext
@commit
def manual_confirm_user(user):
    """Confirm a user."""
    user_obj = _datastore.get_user(user)
    if user_obj is None:
        raise click.UsageError('ERROR: User not found.')
    if confirm_user(user_obj):
        click.secho('User "%s" has been confirmed.' % user, fg='green')
    else:
        click.secho('User "%s" was already confirmed.' % user, fg='yellow')


@click.group()
def utils():
    """Misc management commands."""


utils.add_command(users_validate)
utils.add_command(create_terminal)
utils.add_command(list_terminal)
utils.add_command(update_terminal)


def queue_count():
    """Count tasks in celery."""
    inspector = current_celery.control.inspect()
    task_count = 0
    reserved = inspector.reserved()
    if reserved:
        for _, values in reserved.items():
            task_count += len(values)
    active = inspector.active()
    if active:
        for _, values in active.items():
            task_count += len(values)
    return task_count


def wait_empty_tasks(delay, verbose=False):
    """Wait for tasks to be empty."""
    if verbose:
        spinner = itertools.cycle(['-', '\\', '|', '/'])
        click.echo(
            f'Waiting: {next(spinner)}\r',
            nl=False
        )
    count = queue_count()
    sleep(5)
    count += queue_count()
    while count:
        if verbose:
            click.echo(
                f'Waiting: {next(spinner)}\r',
                nl=False
            )
        sleep(delay)
        count = queue_count()
        sleep(5)
        count += queue_count()


@utils.command('wait_empty_tasks')
@click.option('-d', '--delay', 'delay', default=3)
@with_appcontext
def wait_empty_tasks_cli(delay):
    """Wait for tasks to be empty."""
    wait_empty_tasks(delay=delay, verbose=True)
    click.secho('No active celery tasks.', fg='green')


@utils.command('show')
@click.argument('pid_value', nargs=1)
@click.option('-t', '--pid-type', 'pid-type, default(document_id)',
              default='document_id')
@with_appcontext
def show(pid_value, pid_type):
    """Show records."""
    record = PersistentIdentifier.query.filter_by(pid_type=pid_type,
                                                  pid_value=pid_value).first()
    recitem = Record.get_record(record.object_uuid)
    click.echo(json.dumps(recitem.dumps(), indent=2))


@utils.command('check_json')
@click.argument('paths', nargs=-1)
@click.option(
    '-r', '--replace', 'replace', is_flag=True, default=False,
    help='change file in place default=False'
)
@click.option(
    '-s', '--sort-keys', 'sort_keys', is_flag=True, default=False,
    help='order keys during replacement default=False'
)
@click.option(
    '-i', '--indent', 'indent', type=click.INT, default=2,
    help='indent default=2'
)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
def check_json(paths, replace, indent, sort_keys, verbose):
    """Check json files."""
    click.secho('Testing JSON indentation.', fg='green')
    files_list = []
    for path in paths:
        if os.path.isfile(path):
            files_list.append(path)
        elif os.path.isdir(path):
            files_list = files_list + glob(os.path.join(path, '**/*.json'),
                                           recursive=True)
    if not paths:
        files_list = glob('**/*.json', recursive=True)
    tot_error_cnt = 0
    for path_file in files_list:
        error_cnt = 0
        try:
            fname = path_file
            with open(fname, 'r') as opened_file:
                json_orig = opened_file.read().rstrip()
                opened_file.seek(0)
                json_file = json.load(opened_file,
                                      object_pairs_hook=OrderedDict)
            json_dump = json.dumps(json_file, indent=indent).rstrip()
            if json_dump != json_orig:
                error_cnt = 1
            if replace:
                with open(fname, 'w') as opened_file:
                    opened_file.write(json.dumps(json_file,
                                                 indent=indent,
                                                 sort_keys=sort_keys))
                click.echo(fname + ': ', nl=False)
                click.secho('File replaced', fg='yellow')
            else:
                if error_cnt == 0:
                    if verbose:
                        click.echo(fname + ': ', nl=False)
                        click.secho('Well indented', fg='green')
                else:
                    click.echo(fname + ': ', nl=False)
                    click.secho('Bad indentation', fg='red')
        except ValueError as error:
            click.echo(fname + ': ', nl=False)
            click.secho('Invalid JSON', fg='red', nl=False)
            click.echo(f' -- {error}')
            error_cnt = 1

        tot_error_cnt += error_cnt

    sys.exit(tot_error_cnt)


@utils.command('schedules')
@with_appcontext
def schedules():
    """List harvesting schedules."""
    celery_ext = current_app.extensions.get('invenio-celery')
    for key, value in celery_ext.celery.conf.beat_schedule.items():
        click.echo(key + '\t', nl=False)
        click.echo(value)


@utils.command('init_index')
@click.option('--force', is_flag=True, default=False)
@with_appcontext
@es_version_check
def init_index(force):
    """Initialize registered templates, aliases and mappings."""
    # TODO: to remove once it is fixed in invenio-search module
    click.secho('Putting templates...', fg='green', bold=True, file=sys.stderr)
    with click.progressbar(
            current_search.put_templates(ignore=[400] if force else None),
            length=len(current_search.templates)) as bar:
        for response in bar:
            bar.label = response
    click.secho('Creating indexes...', fg='green', bold=True, file=sys.stderr)
    with click.progressbar(
            current_search.create(ignore=[400] if force else None),
            length=len(current_search.mappings)) as bar:
        for name, response in bar:
            bar.label = name


@utils.command('switch_index')
@with_appcontext
@es_version_check
@click.argument('old')
@click.argument('new')
def switch_index(old, new):
    """Switch index using the elasticsearch aliases.

    :param old: full name of the old index
    :param new: full name of the fresh created index
    """
    aliases = current_search_client.indices.get_alias().get(old)\
        .get('aliases').keys()
    for alias in aliases:
        current_search_client.indices.put_alias(new, alias)
        current_search_client.indices.delete_alias(old, alias)
    click.secho('Sucessfully switched.', fg='green')


@utils.command('create_index')
@with_appcontext
@es_version_check
@click.option(
    '-t', '--templates/--no-templates', 'templates', is_flag=True,
    default=True)
@click.option(
    '-v', '--verbose/--no-verbose', 'verbose', is_flag=True, default=False)
@click.argument('resource')
@click.argument('index')
def create_index(resource, index, verbose, templates):
    """Create a new index based on the mapping of a given resource.

    :param resource: the resource such as documents.
    :param index: the index name such as documents-document-v0.0.1-20211014
    :param verbose: display addtional message.
    :param templates: update also the es templates.
    """
    if templates:
        tbody = current_search_client.indices.get_template()
        for tmpl in current_search.put_templates():
            click.secho(f'file:{tmpl[0]}, ok: {tmpl[1]}', fg='green')
            new_tbody = current_search_client.indices.get_template()
            patch = make_patch(new_tbody, tbody)
            if patch:
                click.secho('Templates are updated.', fg='green')
                if verbose:
                    click.secho('Diff in templates', fg='green')
                    click.echo(patch)
            else:
                click.secho('Templates did not changed.', fg='yellow')

    f_mapping = [
        v for v in current_search.aliases.get(resource).values()].pop()
    mapping = json.load(open(f_mapping))
    current_search_client.indices.create(index, mapping)
    click.secho(f'Index {index} has been created.', fg='green')


@utils.command('update_mapping')
@click.option('--aliases', '-a', multiple=True, help='all if not specified')
@with_appcontext
@es_version_check
def update_mapping(aliases):
    """Update the mapping of a given alias."""
    if not aliases:
        aliases = current_search.aliases.keys()
    for alias in aliases:
        for index, f_mapping in iter(
            current_search.aliases.get(alias).items()
        ):
            mapping = json.load(open(f_mapping))
            try:
                res = current_search_client.indices.put_mapping(
                    mapping.get('mappings'), index)
            except Exception as excep:
                click.secho(
                    f'error: {excep}', fg='red')
            if res.get('acknowledged'):
                click.secho(
                    f'index: {index} has been sucessfully updated', fg='green')
            else:
                click.secho(
                    f'error: {res}', fg='red')


@fixtures.command('create')
@click.option('-u', '--create_or_update', 'create_or_update', is_flag=True,
              default=False)
@click.option('-a', '--append', 'append', is_flag=True, default=False)
@click.option('-r', '--reindex', 'reindex', is_flag=True, default=False)
@click.option('-c', '--dbcommit', 'dbcommit', is_flag=True, default=False)
@click.option('-C', '--commit', 'commit', default=100000)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=True)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@click.option('-s', '--schema', 'schema', default=None)
@click.option('-t', '--pid_type', 'pid_type', default=None)
@click.option('-l', '--lazy', 'lazy', is_flag=True, default=False)
@click.option('-o', '--dont-stop', 'dont_stop_on_error',
              is_flag=True, default=False)
@click.option('-P', '--pid-check', 'pid_check',
              is_flag=True, default=False)
@click.option('-e', '--save_errors', 'save_errors', type=click.File('w'))
@click.argument('infile', type=click.File('r'), default=sys.stdin)
@with_appcontext
def create(infile, create_or_update, append, reindex, dbcommit, commit,
           verbose, debug, schema, pid_type, lazy, dont_stop_on_error,
           pid_check, save_errors):
    """Load REROILS record.

    :param infile: Json file
    :param create_or_update: to update or create records.
    :param append: appends pids to database
    :param reindex: reindex record by record
    :param dbcommit: commit record to database
    :param commit: commit to database every count records
    :param pid_type: record type
    :param schema: recoord schema
    :param lazy: lazy reads file
    :param dont_stop_on_error: don't stop on error
    :param pidcheck: check pids
    :param save_errors: save error records to file
    """
    click.secho(
        f'Loading {pid_type} records from {infile.name}.',
        fg='green'
    )

    record_class = get_record_class_from_schema_or_pid_type(pid_type=pid_type)

    if save_errors:
        errors = 0
        name, ext = os.path.splitext(infile.name)
        err_file_name = f'{name}_errors{ext}'
        error_file = JsonWriter(err_file_name)

    pids = []
    if lazy:
        # try to lazy read json file (slower, better memory management)
        records = read_json_record(infile)
    else:
        # load everything in memory (faster, bad memory management)
        records = json.load(infile)
    count = 0
    for count, record in enumerate(records, 1):
        if schema:
            record['$schema'] = schema
        try:
            pid = record.get('pid')
            msg = 'created'
            db_record = record_class.get_record_by_pid(pid)
            if create_or_update and db_record:
                # case when record already exist in database
                db_record = record_class.get_record_by_pid(pid)
                rec = db_record.update(
                        record, dbcommit=dbcommit, reindex=reindex)
                msg = 'updated'
            elif create_or_update and pid and not db_record \
                    and record_class.record_pid_exists(pid):
                # case when record not in db but pid is reserved
                presist_id = PersistentIdentifier.get(
                    record_class.provider.pid_type, pid)
                rec = record_class.create(
                    record, dbcommit=dbcommit, reindex=reindex)
                if presist_id.status != PIDStatus.REGISTERED:
                    presist_id.register()
                    presist_id.assign(record_class.object_type, rec.id)
                msg = 'created'
            else:
                # case when record and pid are not in db
                rec = record_class.create(
                        record, dbcommit=dbcommit, reindex=reindex,
                        pidcheck=pid_check)
                if append:
                    pids.append(rec.pid)
            if verbose:
                click.echo(
                    f'{count: <8} {pid_type} {msg} {rec.pid}:{rec.id}')

        except Exception as err:
            pid = record.get('pid', '???')
            click.secho(
                f'{count: <8} {type} create error {pid}: {err}',
                fg='red'
            )
            if debug:
                traceback.print_exc()

            if save_errors:
                error_file.write(record)
            if not dont_stop_on_error:
                sys.exit(1)
        db.session.flush()
        if count > 0 and count % commit == 0:
            if verbose:
                click.echo(f'DB commit: {count}')
            db.session.commit()
    click.echo(f'DB commit: {count}')
    db.session.commit()

    if append:
        click.secho(f'Append fixtures new identifiers: {len(pids)}')
        identifier = record_class.provider.identifier
        try:
            append_fixtures_new_identifiers(
                identifier,
                sorted(pids, key=lambda x: int(x)),
                pid_type
            )
        except Exception as err:
            click.secho(
                f'ERROR append fixtures new identifiers: {err}',
                fg='red'
            )


@utils.command('validate_documents_with_items')
@click.argument('infile', type=click.File('r'), default=sys.stdin)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@with_appcontext
def validate_documents_with_items(infile, verbose, debug):
    """Validate REROILS records with items.

    :param infile: Json file
    :param verbose: verbose print
    :param debug: print traceback
    """
    def add_org_lib_doc(item):
        """Add organisation, library and document to item for validation."""
        item['pid'] = 'dummy'
        location_pid = extracted_data_from_ref(item['location'])
        location = Location.get_record_by_pid(location_pid)
        library = Library.get_record_by_pid(location.library_pid)
        item['organisation'] = library.get('organisation')
        item['library'] = location.get('library')
        item['document']['$ref'] = item[
            'document']['$ref'].replace('{document_pid}', '1')
        return item

    click.secho(
        f'Validate documents and items from {infile.name}.',
        fg='green'
    )
    schema_path = current_jsonschemas.url_to_path(
        get_schema_for_resource('doc'))
    schema = current_jsonschemas.get_schema(path=schema_path)
    doc_schema = _records_state.replace_refs(schema)
    schema_path = current_jsonschemas.url_to_path(
        get_schema_for_resource('item'))
    schema = current_jsonschemas.get_schema(path=schema_path)
    item_schema = _records_state.replace_refs(schema)
    doc_pid = next(
        DocumentsSearch().filter('match_all').source('pid').scan()).pid

    document_errors = 0
    item_errors = 0
    for count, record in enumerate(read_json_record(infile), 1):
        pid = record.get('pid')
        items = record.pop('items', [])
        if verbose:
            click.echo(f'{count: <8} document validate {pid}')
        try:
            validate(record, doc_schema)
        except ValidationError as err:
            document_errors += 1
            if debug:
                trace = traceback.format_exc(1)
            else:
                trace = "\n".join(traceback.format_exc(1).split('\n')[:6])
            click.secho(
                f'Error validate in document: {count} {pid} {trace}',
                fg='red'
            )
        for idx, item in enumerate(items, 1):
            if verbose:
                click.echo(f'{"": <12} item validate {idx}')
            try:
                validate(add_org_lib_doc(item), item_schema)
            except ValidationError:
                item_errors += 1
                if debug:
                    trace = traceback.format_exc(1)
                else:
                    trace = "\n".join(traceback.format_exc(1).split('\n')[:6])
                click.secho(
                    f'Error validate in item: {count} {pid} {idx} {trace}',
                    fg='red'
                )
    color = 'green'
    if document_errors or item_errors:
        color = 'red'
    click.secho(
        f'document errors: {document_errors} item errors: {item_errors}',
        fg=color
    )


@utils.command('create_documents_with_items')
@click.argument('infile', type=click.File('r'), default=sys.stdin)
@click.option('-l', '--lazy', 'lazy', is_flag=True, default=False)
@click.option('-o', '--dont-stop', 'dont_stop_on_error',
              is_flag=True, default=False)
@click.option('-e', '--save_errors', 'save_errors', type=click.File('w'))
@click.option('-C', '--commit', 'commit', default=10000)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@with_appcontext
def create_documents_with_items(infile, lazy, dont_stop_on_error, save_errors,
                                commit, verbose, debug):
    """Load REROILS record with items.

    :param infile: Json file
    :param lazy: lazy read file
    :param dont_stop_on_error: don't stop on error
    :param save_errors: save error records to file
    :param commit: commit to database every count records
    :param verbose: verbose print
    :param debug: print traceback
    """
    click.secho(
        f'Loading documents and items from {infile.name}.',
        fg='green'
    )
    name, ext = os.path.splitext(infile.name)
    document_file = JsonWriter(f'{name}_documents{ext}')
    item_file = JsonWriter(f'{name}_items{ext}')
    if save_errors:
        err_file_name = f'{name}_errors{ext}'
        error_file = JsonWriter(err_file_name)

    if lazy:
        # try to lazy read json file (slower, better memory management)
        records = read_json_record(infile)
    else:
        # load everything in memory (faster, bad memory management)
        records = json.load(infile)

    saved_items = []
    document_ids = []
    for count, record in enumerate(records, 1):
        try:
            pid = record.get('pid')
            items = record.pop('items', [])

            # find existing document by ISBN
            def filter_isbn(identified_by):
                """Filter identified_by for type bf:Isbn."""
                return identified_by.get('type') == 'bf:Isbn'

            filtered_identified_by = filter(
                filter_isbn,
                record.get('identifiedBy', [])
            )
            isbns = set()
            for identified_by in filtered_identified_by:
                isbn = identified_by['value']
                isbns.add(isbn)
            isbns = list(isbns)

            search = DocumentsSearch().filter('terms', isbn=isbns)
            exists = search.count()

            rec = Document.create(data=record, delete_pid=True)
            document_file.write(rec)
            document_ids.append(rec.id)
            if verbose:
                click.echo(
                    f'{count: <8} document created '
                    f'{rec.pid}:{rec.id} {exists}')
            for item in items:
                # change the document pid
                # "document": {
                # "$ref":
                #   "https://bib.rero.ch/api/documents/{document_pid}"}
                item['document']['$ref'] = item[
                    'document']['$ref'].replace('{document_pid}', rec.pid)
                saved_items.append({
                    'count': count,
                    'pid': pid,
                    'rero_ils_pid': rec.pid,
                    'item': item
                })

        except Exception as err:
            click.secho(
                f'{count: <8} create error {pid}'
                f' {record.get("pid")}: {err}',
                fg='red'
            )
            if debug:
                traceback.print_exc()

            if save_errors:
                error_file.write(record)
            if not dont_stop_on_error:
                sys.exit(1)

        if count % commit == 0:
            db.session.commit()
            DocumentsIndexer().bulk_index(document_ids)
            process_bulk_queue()
            for item in saved_items:
                item_rec = Item.create(data=item['item'], delete_pid=True,
                                       dbcommit=True, reindex=True)
                item_file.write(item_rec)
                if verbose:
                    click.echo(
                        f'         - {item["count"]: <8}'
                        f' doc:{item["rero_ils_pid"]}'
                        f' item created {item_rec.pid}:{item_rec.id}')
            saved_items = []
            docuemnt_items = []
    db.session.commit()
    DocumentsIndexer().bulk_index(document_ids)
    process_bulk_queue()
    for item in saved_items:
        item_rec = Item.create(data=item['item'], delete_pid=True,
                               dbcommit=True, reindex=True)
        item_file.write(item_rec)
        if verbose:
            click.echo(
                f'         - {item["count"]: <8}'
                f' doc:{item["rero_ils_pid"]}'
                f' item created {item_rec.pid}:{item_rec.id}')


@fixtures.command('count')
@click.option('-l', '--lazy', 'lazy', is_flag=True, default=False)
@click.argument('infile', type=click.File('r'), default=sys.stdin)
def count_cli(infile, lazy):
    """Count records in file.

    :param infile: Json file
    :param lazy: lazy reads file
    :return: count of records
    """
    click.secho(
        f'Count records from {infile.name}.',
        fg='green'
    )
    if lazy:
        # try to lazy read json file (slower, better memory management)
        records = read_json_record(infile)
    else:
        # load everything in memory (faster, bad memory management)
        records = json.load(infile)
    count = 0
    for record in records:
        count += 1
    click.echo(f'Count: {count}')


@fixtures.command('get_all_mef_records')
@click.argument('infile', type=click.File('r'), default=sys.stdin)
@click.option('-l', '--lazy', 'lazy', is_flag=True, default=False,
              help="lazy reads file")
@click.option('-k', '--enqueue', 'enqueue', is_flag=True, default=False,
              help="Enqueue record creation.")
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=True,
              help='verbose')
@click.option('-w', '--wait', 'wait', is_flag=True, default=False,
              help="wait for enqueued tasks to finish")
@click.option('-o', '--out_file', 'outfile_name', default=None)
@with_appcontext
def get_all_mef_records(infile, lazy, verbose, enqueue, wait, outfile_name):
    """Get all contributions for given document file."""
    click.secho(
        f'Get all contributions for {infile.name}.',
        fg='green'
    )
    if outfile_name:
        outfile = JsonWriter(outfile_name)
        contribution_schema = get_schema_for_resource('cont')
        click.secho(f'Write to: {outfile_name}.')
    if lazy:
        # try to lazy read json file (slower, better memory management)
        records = read_json_record(infile)
    else:
        # load everything in memory (faster, bad memory management)
        records = json.load(infile)
    count = 0
    refs = {}
    for count, record in enumerate(records, 1):
        for contribution in record.get('contribution', []):
            ref = contribution['agent'].get('$ref')
            if ref and not refs.get(ref):
                refs[ref] = 1
                if outfile_name:
                    try:
                        ref_split = ref.split('/')
                        ref_type = ref_split[-2]
                        ref_pid = ref_split[-1]
                        data = Contribution._get_mef_data_by_type(
                            pid=ref_pid,
                            pid_type=ref_type
                        )
                        metadata = data['metadata']
                        metadata['$schema'] = contribution_schema
                        outfile.write(metadata)
                        msg = 'ok'
                    except Exception as err:
                        msg = err
                else:
                    if enqueue:
                        msg = create_mef_record_online.delay(ref)
                    else:
                        pid, online = create_mef_record_online(ref)
                        msg = f'contribution pid: {pid} {online}'
                if verbose:
                    click.echo(f"{count:<10}ref: {ref}\t{msg}")
    else:
        if enqueue and wait:
            wait_empty_tasks(delay=3, verbose=True)
    click.echo(f'Count refs: {count}')


@utils.command('check_license')
@click.argument('configfile', type=click.File('r'), default=sys.stdin)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-p', '--progress', 'progress', is_flag=True, default=False)
def check_license(configfile, verbose, progress):
    """Check licenses."""
    click.secho('Testing licenses in files.', fg='green')

    def get_files(paths, extensions, recursive=True):
        """Get files from paths."""
        files_list = []
        for path in paths:
            if os.path.isfile(path):
                files_list.append(path)
            elif os.path.isdir(path):
                for extension in extensions:
                    files_list += glob(
                        os.path.join(path, f'**/*.{extension}'),
                        recursive=recursive
                    )
        return files_list

    def delete_prefix(prefix, line):
        """Delete prefix from line."""
        if prefix:
            line = line.replace(prefix, "")
        return line.strip()

    def is_copyright(line):
        """Line is copyright."""
        if line.startswith('Copyright (C)'):
            return True
        return False

    def get_line(lines, index, prefix):
        """Get line on index."""
        line = delete_prefix(prefix, lines[index])
        return line, index+1

    def show_diff(linenbr, text, n_text):
        """Show string diffs."""
        seqm = difflib.SequenceMatcher(
            None,
            text.replace(' ', '◼︎'),
            n_text.replace(' ', '◼︎')
        )
        click.echo(f'{linenbr}: ', nl=False)
        for opcode, a0, a1, b0, b1 in seqm.get_opcodes():
            if opcode == 'equal':
                click.echo(seqm.a[a0:a1], nl=False)
            elif opcode == 'insert':
                click.secho(seqm.b[b0:b1], fg='red', nl=False)
            elif opcode == 'delete':
                click.secho(seqm.a[a0:a1], fg='blue', nl=False)
            elif opcode == 'replace':
                # seqm.a[a0:a1] -> seqm.b[b0:b1]
                click.secho(seqm.b[b0:b1], fg='green', nl=False)
        click.echo()

    def test_file(file_name, extensions, extension, license_lines,
                  verbose, progress):
        """Test the license in file."""
        if progress:
            click.secho('License test: ', fg='green', nl=False)
            click.echo(file_name)
        with open(file_name, 'r') as file:
            result = test_license(
                file=file,
                extension=extensions[extension],
                license_lines=license_lines,
                verbose=verbose
            )
            if result != []:
                click.secho(
                    f'License error in {file_name} in lines {result}',
                    fg='red'
                )
                # We have an error
                return 1
        # No error found
        return 0

    def is_slash_directive(file, line):
        is_js_file = file.name.split('.')[-1] == 'js'
        if is_js_file and re.search(triple_slash, line):
            return True
        return False

    def test_license(file, extension, license_lines, verbose):
        """Test the license in file."""
        lines_with_errors = []
        lines = [line.rstrip() for line in file]
        linenbr = 0
        linemaxnbr = len(lines)
        prefix = extension.get('prefix')
        line, linenbr = get_line(lines, linenbr, prefix)
        # Get over Shebang lines or Triple-Slash Directives (for Javascript
        # files)
        while lines[linenbr-1].startswith('#!') or \
                is_slash_directive(file, lines[linenbr-1]):
            # get over Shebang
            line, linenbr = get_line(lines, linenbr, prefix)
        if extension.get('top'):
            # read the top
            if line not in extension.get('top'):
                if verbose:
                    for t in extension['top']:
                        show_diff(linenbr, t, line)
                lines_with_errors.append(linenbr+1)
        line, linenbr = get_line(lines, linenbr, prefix)
        for license_line in license_lines:
            # compare the license lines
            if is_copyright(license_line):
                while is_copyright(line):
                    line, linenbr = get_line(lines, linenbr, prefix)
                linenbr -= 1
                line = 'Copyright (C)'
            if license_line != line:
                if verbose:
                    show_diff(linenbr, license_line, line)
                lines_with_errors.append(linenbr)
            # Fix crash while testing a file with only comments.
            if linenbr >= linemaxnbr:
                continue
            line, linenbr = get_line(lines, linenbr, prefix)
        return lines_with_errors

    config = yaml.safe_load(configfile)
    file_extensions = config['file_extensions']
    extensions = {}
    for file_extension in file_extensions:
        for ext in file_extension.split(','):
            extensions.setdefault(ext.strip(), file_extensions[file_extension])
    # create recursive file list
    files_list = get_files(
        paths=config['directories']['recursive'],
        extensions=extensions,
        recursive=True
    )
    # add flat file list
    files_list += get_files(
        paths=config['directories']['flat'],
        extensions=extensions,
        recursive=False
    )
    # remove excluded files
    exclude_list = []
    for ext in config['directories']['exclude']:
        exclude_list += get_files(
            paths=config['directories']['exclude'][ext],
            extensions=[ext],
            recursive=True
        )
    files_list = list(set(files_list) - set(exclude_list))

    # set regexp expression for Triple-Slash directives
    triple_slash = r'^/// <reference \w*=\"\w*\" />$'

    license_lines = config['license_text'].split('\n')
    tot_error_cnt = 0
    for file_name in files_list:
        # test every file
        extension = os.path.splitext(file_name)[1][1:]
        tot_error_cnt += test_file(
            file_name=file_name,
            extensions=extensions,
            extension=extension,
            license_lines=license_lines,
            verbose=verbose,
            progress=progress
        )
    for extension in config['files']:
        # test every files
        for file_name in config['files'][extension]:
            tot_error_cnt += test_file(
                file_name=file_name,
                extensions=extensions,
                extension=extension,
                license_lines=license_lines,
                verbose=verbose,
                progress=progress
            )

    sys.exit(tot_error_cnt)


@utils.command('validate')
@click.argument('jsonfile', type=click.File('r'))
@click.argument('type', default='doc')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@click.option('-e', '--error_file', 'error_file_name', default=None,
              help='error file')
@click.option('-o', '--ok_file', 'ok_file_name', default=None,
              help='ok file')
@with_appcontext
def check_validate(jsonfile, type, verbose, debug, error_file_name,
                   ok_file_name):
    """Check record validation."""
    click.secho(
        f'Testing json schema for file: {jsonfile.name} type: {type}',
        fg='green'
    )

    schema_path = current_jsonschemas.url_to_path(
        get_schema_for_resource(type))
    schema = current_jsonschemas.get_schema(path=schema_path)
    schema = _records_state.replace_refs(schema)

    datas = json.load(jsonfile)
    count = 0
    if error_file_name:
        error_file = JsonWriter(error_file_name)
    if ok_file_name:
        ok_file = JsonWriter(ok_file_name)
    for count, data in enumerate(datas, 1):
        if verbose:
            click.echo(f'\tTest record: {count}')
        if not data.get('$schema'):
            scheme = current_app.config.get('JSONSCHEMAS_URL_SCHEME')
            host = current_app.config.get('JSONSCHEMAS_HOST')
            endpoint = current_app.config.get('JSONSCHEMAS_ENDPOINT')
            url_schema = f'{scheme}://{host}{endpoint}{schema_path}'
            data['$schema'] = url_schema
        if not data.get('pid'):
            # create dummy pid in data
            data['pid'] = 'dummy'
        try:
            validate(data, schema)
            if ok_file_name:
                if data['pid'] == 'dummy':
                    del data['pid']
                ok_file.write(data)
        except ValidationError:
            trace_lines = traceback.format_exc(1).split('\n')
            trace = trace_lines[5].strip()
            click.secho(
                f'Error validate in record: {count} {trace}',
                fg='red'
            )
            if error_file_name:
                error_file.write(data)
            if debug:
                pprint(data)


def do_worker(marc21records, results, pid_required, debug, schema=None):
    """Worker for marc21 to json transformation."""
    for data in marc21records:
        data_json = data['json']
        pid = data_json.get('001', '???')
        record = {}
        try:
            record = marc21.do(data_json)
            if not record.get("$schema"):
                # create dummy schema in data
                record["$schema"] = 'dummy'
            if not pid_required:
                if not record.get("pid"):
                    # create dummy pid in data
                    record["pid"] = 'dummy'
            if schema:
                validate(record, schema)
            if record["$schema"] == 'dummy':
                del record["$schema"]
            if not pid_required:
                if record["pid"] == 'dummy':
                    del record["pid"]
            results.append({
                'status': True,
                'data': record
            })
        except ValidationError as err:
            if debug:
                pprint(record)
            trace_lines = traceback.format_exc(1).split('\n')
            trace = trace_lines[5].strip()
            rero_pid = data_json.get('035__', {}).get('a'),
            msg = f'ERROR:\t{pid}\t{rero_pid}\t{err.args[0]}\t-\t{trace}'
            click.secho(msg, fg='red')
            results.append({
                'pid': pid,
                'status': False,
                'data': data['xml']
            })
        except Exception as err:
            rero_pid = data_json.get('035__', {}).get('a'),
            msg = f'ERROR:\t{pid}\t{rero_pid}\t{err.args[0]}'
            click.secho(msg, fg='red')
            if debug:
                traceback.print_exc()
            results.append({
                'pid': pid,
                'status': False,
                'data': data['xml']
            })


class Marc21toJson():
    """Class for Marc21 recorts to Json transformation."""

    __slots__ = ['xml_file', 'json_file_ok', 'xml_file_error', 'parallel',
                 'chunk', 'verbose', 'debug', 'pid_required',
                 'count', 'count_ok', 'count_ko', 'ctx',
                 'results', 'active_buffer', 'buffer', 'first_result',
                 'schema']

    def __init__(self, xml_file, json_file_ok, xml_file_error,
                 parallel=8, chunk=10000,
                 verbose=False, debug=False, pid_required=False, schema=None):
        """Constructor."""
        self.count = 0
        self.count_ok = 0
        self.count_ko = 0
        self.xml_file = xml_file
        self.json_file_ok = json_file_ok
        self.xml_file_error = xml_file_error
        self.parallel = parallel
        self.chunk = chunk
        self.verbose = verbose
        self.schema = schema
        self.first_result = True
        if verbose:
            click.echo(
                f'Main process pid: {multiprocessing.current_process().pid}')
        self.debug = debug
        if debug:
            multiprocessing.log_to_stderr(logging.DEBUG)
        self.pid_required = pid_required
        self.ctx = multiprocessing.get_context("spawn")
        manager = self.ctx.Manager()
        self.results = manager.list()
        self.active_buffer = 0
        self.buffer = []
        for index in range(parallel):
            self.buffer.append({'process': None, 'records': []})
        self.start()

    def counts(self):
        """Get the counters."""
        return self.count, self.count_ok, self.count_ko

    def write_results(self):
        """Write results from multiprocess to file."""
        while self.results:
            value = self.results.pop(0)
            status = value.get('status')
            data = value.get('data')
            if status:
                self.count_ok += 1
                if self.first_result:
                    self.first_result = False
                else:
                    self.json_file_ok.write(',')
                for line in json.dumps(data, indent=2).split('\n'):
                    self.json_file_ok.write('\n  ' + line)
            else:
                self.count_ko += 1
                self.xml_file_error.write(data)

    def wait_free_process(self):
        """Wait for next process to finish."""
        index = (self.active_buffer + 1) % self.parallel
        process = self.buffer[index]['process']
        if process:
            process.join()
        # reset data for finished jobs
        for index in range(self.parallel):
            process = self.buffer[index].get('process')
            if process and process.exitcode is not None:
                del self.buffer[index]['process']
                self.buffer[index].clear()
                self.buffer[index] = {'process': None, 'records': []}

    def next_active_buffer(self):
        """Set the next active buffer index."""
        self.active_buffer = (self.active_buffer + 1) % self.parallel

    def wait_all_free_process(self):
        """Wait for all processes to finish."""
        for index in range(self.parallel):
            self.wait_free_process()
            self.next_active_buffer()

    def start_new_process(self):
        """Start a new process in context."""
        new_process = self.ctx.Process(
            target=do_worker,
            args=(self.active_records, self.results, self.pid_required,
                  self.debug, self.schema)
        )
        self.wait_free_process()
        new_process.start()
        self.active_process = new_process
        if self.verbose:
            if self.count < self.chunk:
                start = 1
            else:
                start = self.count - len(self.active_records) + 1
            pid = new_process.pid
            click.echo(f'Start process: {pid} records: {start}..{self.count}')
        self.next_active_buffer()

    def write_start(self):
        """Write initial lines to files."""
        self.json_file_ok.write('[')
        self.xml_file_error.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        self.xml_file_error.write(
            b'<collection xmlns="http://www.loc.gov/MARC21/slim">\n\n'
        )

    def write_stop(self):
        """Write finishing lines to files."""
        self.json_file_ok.write('\n]')
        self.xml_file_error.write(b'\n</collection>')

    def start(self):
        """Start the transformation."""
        self.write_start()
        for marc21xml in read_xml_record(self.xml_file):
            marc21json_record = create_record(marc21xml)
            self.active_records.append({
                'json': marc21json_record,
                'xml': etree.tostring(
                    marc21xml,
                    pretty_print=True,
                    encoding='UTF-8'
                ).strip()
            })
            self.count += 1
            if len(self.active_records) % self.chunk == 0:
                self.write_results()
                self.start_new_process()

        # process the remaining records
        self.write_results()
        if self.active_records:
            self.start_new_process()
        self.wait_all_free_process()
        self.write_results()
        self.write_stop()
        return self.count, self.count_ok, self.count_ko

    @property
    def active_process(self):
        """Get the active process."""
        return self.buffer[self.active_buffer]['process']

    @active_process.setter
    def active_process(self, process):
        """Set the active process."""
        self.buffer[self.active_buffer]['process'] = process

    @property
    def active_records(self):
        """Get the active records."""
        return self.buffer[self.active_buffer]['records']


@utils.command('marc21tojson')
@click.argument('xml_file', type=click.File('r'))
@click.argument('json_file_ok', type=click.File('w'))
@click.argument('xml_file_error', type=click.File('wb'))
@click.option('-p', '--parallel', 'parallel', default=8)
@click.option('-c', '--chunk', 'chunk', default=10000)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-d', '--debug', 'debug', is_flag=True, default=False)
@click.option('-r', '--pidrequired', 'pid_required', is_flag=True,
              default=False)
@with_appcontext
def marc21json(xml_file, json_file_ok, xml_file_error, parallel, chunk,
               verbose, debug, pid_required):
    """Convert xml file to json with dojson."""
    click.secho('Marc21 to Json transform: ', fg='green', nl=False)
    if pid_required and verbose:
        click.secho(' (validation tests pid) ', nl=False)
    click.secho(xml_file.name)

    path = current_jsonschemas.url_to_path(get_schema_for_resource('doc'))
    schema = current_jsonschemas.get_schema(path=path)
    schema = _records_state.replace_refs(schema)
    transform = Marc21toJson(xml_file, json_file_ok, xml_file_error, parallel,
                             chunk, verbose, debug, pid_required, schema)

    count, count_ok, count_ko = transform.counts()

    click.secho('Total records: ', fg='green', nl=False)
    click.secho(str(count), nl=False)
    click.secho('-', nl=False)
    click.secho(str(count_ok + count_ko))

    click.secho('Records transformed: ', fg='green', nl=False)
    click.secho(str(count_ok))
    if count_ko:
        click.secho('Records with errors: ', fg='red', nl=False)
        click.secho(str(count_ko))


@utils.command('extract_from_xml')
@click.argument('pid_file', type=click.File('r'))
@click.argument('xml_file_in', type=click.File('r'))
@click.argument('xml_file_out', type=click.File('wb'))
@click.option('-t', '--tag', 'tag', default='001')
@click.option('-p', '--progress', 'progress', is_flag=True, default=False)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
def extract_from_xml(pid_file, xml_file_in, xml_file_out, tag, progress,
                     verbose):
    """Extracts xml records with pids."""
    click.secho('Extract pids from xml: ', fg='green')
    click.secho(f'PID file    : {pid_file.name}')
    click.secho(f'XML file in : {xml_file_in.name}')
    click.secho(f'XML file out: {xml_file_out.name}')

    pids = {}
    found_pids = {}
    for line in pid_file:
        pids[line.strip()] = 0
    count = len(pids)
    click.secho(f'Search pids count: {count}')
    xml_file_out.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
    xml_file_out.write(
        b'<collection xmlns="http://www.loc.gov/MARC21/slim">\n\n'
    )
    found = 0
    for idx, xml in enumerate(read_xml_record(xml_file_in)):
        for child in xml:
            is_controlfield = child.tag == 'controlfield'
            is_tag = child.get('tag') == tag
            if is_controlfield and is_tag:
                if progress:
                    click.secho(
                        f'{idx} {repr(child.text)}',
                        nl='\r'
                    )
                if pids.get(child.text, -1) >= 0:
                    found += 1
                    pids[child.text] += 1
                    data = etree.tostring(
                        xml,
                        pretty_print=True,
                        encoding='UTF-8'
                    ).strip()

                    xml_file_out.write(data)
                    found_pids[child.text] = True
                    if verbose:
                        click.secho(f'Found: {child.text} on position: {idx}')
                    break
    xml_file_out.write(b'\n</collection>')
    if count != found:
        click.secho(f'Count: {count} Found: {found}', fg='red')
        for key, value in pids.items():
            if value == 0:
                click.secho(key)


@utils.command('reserve_pid_range')
@click.option('-t', '--pid_type', 'pid_type', default=None,
              help='pid type of the resource')
@click.option('-n', '--records_number', 'records_number', default=None,
              help='Number of records to load')
@click.option('-u', '--unused', 'unused', is_flag=True, default=False,
              help='Set unused (gaps) pids status to NEW ')
@with_appcontext
def reserve_pid_range(pid_type, records_number, unused):
    """Reserve a range of pids for future records loading.

    reserved pids will have the status RESERVED.
    - pid_type: the pid type of the resource as configured in config.py
    - records_number: number of new records(with pids) to load.
    - unused: set that the status of unused (gaps) pids to NEW.
    """
    click.secho('Reserving pids for loading "%s" records' %
                pid_type, fg='green')
    try:
        records_number = int(records_number)
    except ValueError:
        raise ValueError('Parameter records_number must be integer.')

    record_class = get_record_class_from_schema_or_pid_type(pid_type=pid_type)
    if not record_class:
        raise AttributeError('Invalid pid type.')

    identifier = record_class.provider.identifier
    reserved_pids = []
    for number in range(0, records_number):
        pid = identifier.next()
        reserved_pids.append(pid)
        record_class.provider.create(pid_type, pid_value=pid,
                                     status=PIDStatus.RESERVED)
        db.session.commit()
    min_pid = min(reserved_pids)
    max_pid = max(reserved_pids)
    click.secho(f'reserved_pids range, from: {min_pid} to: {max_pid}')
    if unused:
        for pid in range(1, identifier.max()):
            if not db.session.query(
                    identifier.query.filter(identifier.recid == pid).exists()
            ).scalar():
                record_class.provider.create(pid_type, pid_value=pid,
                                             status=PIDStatus.NEW)
                db.session.add(identifier(recid=pid))
            db.session.commit()


@utils.command('runindex')
@click.option(
    '--delayed', '-d', is_flag=True, help='Run indexing in background.')
@click.option(
    '--concurrency', '-c', default=1, type=int,
    help='Number of concurrent indexing tasks to start.')
@click.option(
    '--with_stats', is_flag=True, default=False,
    help='report number of successful and list failed error response.')
@click.option('--queue', '-q', type=str,
              help='Name of the celery queue used to put the tasks into.')
@click.option('--version-type', help='Elasticsearch version type to use.')
@click.option(
    '--raise-on-error/--skip-errors', default=True,
    help='Controls if Elasticsearch bulk indexing errors raise an exception.')
@with_appcontext
def run(delayed, concurrency, with_stats, version_type=None, queue=None,
        raise_on_error=True):
    """Run bulk record indexing."""
    if delayed:
        celery_kwargs = {
            'kwargs': {
                'version_type': version_type,
                'es_bulk_kwargs': {'raise_on_error': raise_on_error},
                'stats_only': not with_stats
            }
        }
        click.secho(
            f'Starting {concurrency} tasks for indexing records...',
            fg='green'
        )
        if queue is not None:
            celery_kwargs.update({'queue': queue})
        for c in range(0, concurrency):
            process_id = process_bulk_queue.apply_async(**celery_kwargs)
            click.secho(f'index async: {process_id}', fg='yellow')

    else:
        click.secho('Indexing records...', fg='green')
        indexed, error = IlsRecordsIndexer(version_type=version_type)\
            .process_bulk_queue(
                es_bulk_kwargs={'raise_on_error': raise_on_error},
                stats_only=not with_stats
            )
        click.secho(f'indexed: {indexed}, error: {error}', fg='yellow')


@utils.command('reindex')
@click.option('--yes-i-know', is_flag=True, callback=abort_if_false,
              expose_value=False,
              prompt='Do you really want to reindex records?')
@click.option('-t', '--pid-types', multiple=True)
@click.option('-n', '--no-info', 'no_info', is_flag=True, default=True)
@click.option('-f', '--from_date', 'from_date')
@click.option('-u', '--until_date', 'until_date')
@click.option('-d', '--direct', 'direct', is_flag=True, default=False)
@click.option('-c', '--count', 'count', is_flag=True, default=False)
@click.option('-i', '--index', 'index')
@with_appcontext
def reindex(pid_types, no_info, from_date, until_date, direct, count, index):
    """Reindex records.

    :param pid_type: Pid type.
    :param no_info: Display no info.
    :param from_date: Index records from date.
    :param until_date: Index records until date.
    :param direct: Use record class for indexing.
    :param count: Do not index, display only counts.
    :param index: Index name to index.
    """
    endpoints = current_app.config.get('RECORDS_REST_ENDPOINTS')
    if not pid_types:
        pid_types = [endpoint for endpoint in endpoints]
    for pid_type in pid_types:
        if pid_type in endpoints:
            msg = f'Sending {pid_type} to indexing queue: '
            if count:
                msg = f'Count {pid_type:>6}: '
            click.secho(msg, fg='green', nl=False)
            query = None
            record_cls = obj_or_import_string(
                endpoints[pid_type].get('record_class'))
            if from_date or until_date:
                model_cls = record_cls.model_cls
                if model_cls != RecordMetadata:
                    query = model_cls.query \
                        .filter(model_cls.is_deleted.is_(False)) \
                        .with_entities(model_cls.id) \
                        .order_by(model_cls.created)
                    if from_date:
                        query = query.filter(
                            model_cls.updated > dateparser.parse(from_date))
                    if until_date:
                        query = query.filter(
                            model_cls.updated <= dateparser.parse(until_date))
            else:
                query = PersistentIdentifier.query \
                    .filter_by(object_type='rec', status=PIDStatus.REGISTERED)\
                    .filter_by(pid_type=pid_type) \
                    .with_entities(PersistentIdentifier.object_uuid)
            if query:
                click.echo(f'{query.count()}')
                if not count:
                    if direct:
                        for idx, id in enumerate((x[0] for x in query), 1):
                            msg = f'{idx}\t{id}\t'
                            try:
                                rec = record_cls.get_record_by_id(id)
                                msg += f'{rec.pid}'
                                rec.reindex()
                            except Exception as err:
                                msg += f'\t{err}'
                            click.echo(msg)
                    else:
                        IlsRecordsIndexer().bulk_index(
                            (x[0] for x in query),
                            doc_type=pid_type, index=index)
            else:
                click.echo('Can not index by date.')
        else:
            click.secho(f'ERROR type does not exist: {pid_type}', fg='red')
    if no_info and not direct and not count:
        click.secho(
            'Execute "runindex" command to process the queue!',
            fg='yellow'
        )


@utils.command('reindex_missing')
@click.option('-t', '--pid-types', multiple=True, required=True)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@with_appcontext
def reindex_missing(pid_types, verbose):
    """Index all missing records.

    :param pid_type: Pid type.
    """
    for p_type in pid_types:
        click.secho(
            f'Indexing missing {p_type}: ',
            fg='green',
            nl=False
        )
        record_class = get_record_class_from_schema_or_pid_type(
            pid_type=p_type
        )
        if not record_class:
            click.secho(
                'ERROR pid type does not exist!',
                fg='red',
            )
            continue
        pids_es, pids_db, pids_es_double, index = \
            Monitoring.get_es_db_missing_pids(p_type)
        click.secho(
            f'{len(pids_db)}',
            fg='green',
        )
        for idx, pid in enumerate(pids_db, 1):
            record = record_class.get_record_by_pid(pid)
            if record:
                record.reindex()
                if verbose:
                    click.secho(f'{idx}\t{p_type}\t{pid}')
            else:
                if verbose:
                    click.secho(f'NOT FOUND: {idx}\t{p_type}\t{pid}', fg='red')


@utils.command('check_pid_dependencies')
@click.option('-i', '--dependency_file', 'dependency_file',
              type=click.File('r'), default='./data/pid_dependencies_big.json')
@click.option('-d', '--directory', 'directory', default='./data')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
def check_pid_dependencies(dependency_file, directory, verbose):
    """Check record dependencies."""
    class Dependencies():
        """Class for dependencies checking."""

        test_data = {}

        def __init__(self, directory, verbose=False):
            """Init dependency class."""
            self.directory = directory
            self.verbose = verbose
            self.record = {}
            self.name = ''
            self.pid = '0'
            self.dependencies_pids = []
            self.dependencies = set()
            self.missing = 0
            self.not_found = 0

        def get_pid(self, data):
            """Get pid from end of $ref string."""
            return data['$ref'].split('/')[-1]

        def get_pid_type(self, data):
            """Get pid and type from end of $ref string."""
            data_split = data['$ref'].split('/')
            return data_split[-1], data_split[-2]

        def get_ref_pids(self, data, dependency_name):
            """Get pids from data."""
            pids = []
            try:
                if isinstance(data[dependency_name], list):
                    for dat in data[dependency_name]:
                        pids.append(self.get_pid(dat))
                else:
                    pids = [self.get_pid(data[dependency_name])]
            except Exception as err:
                pass
            return pids

        def get_ref_type_pids(self, data, dependency_name, ref_type):
            """Get pids from data."""
            pids = []
            try:
                if isinstance(data[dependency_name], list):
                    for dat in data[dependency_name]:
                        pid, pid_type = self.get_pid_type(dat)
                        if pid_type == ref_type:
                            pids.append(pid)
                else:
                    pid, pid_type = self.get_pid_type(data[dependency_name])
                    if pid_type == ref_type:
                        pids.append(pid)
            except Exception as err:
                pass
            return pids

        def add_pids_to_dependencies(self, dependency_name, pids, optional):
            """Add pids to dependoencies_pid."""
            if not (pids or optional):
                click.secho(
                    f'{self.name}: dependencies not found: {dependency_name}',
                    fg='red'
                )
                self.not_found += 1
            else:
                self.dependencies_pids.append({
                    dependency_name: pids
                })
                self.dependencies.add(dependency_name)

        def set_dependencies_pids(self, dependencies):
            """Get all dependencies and pids."""
            self.dependencies_pids = []
            for dependency in dependencies:
                dependency_ref = dependency.get('ref')
                dependency_refs = dependency.get('refs')
                if not dependency_ref:
                    dependency_ref = dependency['name']
                sublist = dependency.get('sublist', [])
                for sub in sublist:
                    datas = self.record.get(dependency['name'], [])
                    if not(datas or dependency.get('optional')):
                        click.secho(
                            f'{self.name}: sublist not found: '
                            f'{dependency["name"]}',
                            fg='red'
                        )
                        self.not_found += 1
                    else:
                        for data in datas:
                            dependency_ref = sub.get('ref')
                            if not dependency_ref:
                                dependency_ref = sub['name']
                            self.add_pids_to_dependencies(
                                dependency_ref,
                                self.get_ref_pids(data, sub['name']),
                                sub.get('optional')
                            )
                if not sublist:
                    if dependency_refs:
                        for ref, ref_type in dependency_refs.items():
                            pids = self.get_ref_type_pids(
                                self.record,
                                dependency['name'],
                                ref_type
                            )
                            self.add_pids_to_dependencies(
                                ref,
                                pids,
                                dependency.get('optional')
                            )
                    else:
                        self.add_pids_to_dependencies(
                            dependency_ref,
                            self.get_ref_pids(self.record, dependency['name']),
                            dependency.get('optional')
                        )

        def test_dependencies(self):
            """Test all dependencies."""
            for dependency in self.dependencies_pids:
                for key, values in dependency.items():
                    for value in values:
                        try:
                            self.test_data[key][value]
                        except Exception:
                            click.secho(
                                f'{self.name}: {self.pid} missing '
                                f'{key}: {value}',
                                fg='red'
                            )
                            self.missing += 1

        def init_and_test_data(self, test):
            """Init data and test data."""
            self.name = test['name']
            file_name = os.path.join(self.directory, test['filename'])
            self.test_data.setdefault(self.name, {})
            with open(file_name, 'r') as infile:
                if self.verbose:
                    click.echo(f'{self.name}: {file_name}')
                records = read_json_record(infile)
                for idx, self.record in enumerate(records, 1):
                    self.pid = self.record.get('pid', idx)
                    if self.test_data[self.name].get(self.pid):
                        click.secho(
                            f'Double pid in {self.name}: {self.pid}',
                            fg='red'
                        )
                    else:
                        self.test_data[self.name][self.pid] = {}
                        self.set_dependencies_pids(
                            test.get('dependencies', [])
                        )
                        self.test_dependencies()
                if self.verbose:
                    for dependency in self.dependencies:
                        click.echo(f'\tTested dependency: {dependency}')

        def run_tests(self, tests):
            """Run the tests."""
            for test in tests:
                self.init_and_test_data(test)
            if self.missing:
                click.secho(f'Missing relations: {self.missing}', fg='red')
            if self.not_found:
                click.secho(f'Relation not found: {self.not_found}', fg='red')

    # start of tests
    click.secho(
        f'Check dependencies {dependency_file.name}: {directory}',
        fg='green'
    )
    dependency_tests = Dependencies(directory, verbose=verbose)
    tests = json.load(dependency_file)
    dependency_tests.run_tests(tests)

    sys.exit(dependency_tests.missing + dependency_tests.not_found)


@utils.command('dump_es_mappings')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-o', '--outfile', 'outfile', type=click.File('w'), default=None)
@with_appcontext
def dump_es_mappings(verbose, outfile):
    """Dumps ES mappings."""
    click.secho('Dump ES mappings:', fg='green')
    aliases = current_search.client.indices.get_alias('*')
    mappings = current_search.client.indices.get_mapping()
    for alias in sorted(aliases):
        if alias[0] != '.':
            mapping = mappings.get(alias, {}).get('mappings')
            click.echo(alias)
            if verbose or not outfile:
                print(json.dumps(mapping, indent=2))
            if outfile:
                outfile.write(f'{alias}\n')
                json.dump(mapping, outfile, indent=2)
                outfile.write('\n')


@utils.command('export')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-t', '--pid_type', 'pid_type', default='doc')
@click.option('-o', '--outfile', 'outfile_name', required=True)
@click.option('-i', '--pidfile', 'pidfile', type=click.File('r'),
              default=None)
@click.option('-I', '--indent', 'indent', type=click.INT, default=2)
@click.option('-s', '--schema', 'schema', is_flag=True, default=False)
@with_appcontext
def export(verbose, pid_type, outfile_name, pidfile, indent, schema):
    """Export REROILS record.

    :param verbose: verbose
    :param pid_type: record type
    :param outfile: Json output file
    :param pidfile: files with pids to extract
    :param indent: indent for output
    :param schema: do not delete $schema
    """
    click.secho(f'Export {pid_type} records: {outfile_name}', fg='green')
    outfile = JsonWriter(outfile_name)
    record_class = get_record_class_from_schema_or_pid_type(pid_type=pid_type)

    if pidfile:
        pids = list(filter(None, [line.rstrip() for line in pidfile]))
    else:
        pids = record_class.get_all_pids()

    count = 0
    output = '['
    offset = '{character:{indent}}'.format(character=' ', indent=indent)
    for pid in pids:
        try:
            rec = record_class.get_record_by_pid(pid)
            count += 1
            if verbose:
                click.echo(
                    f'{count: <8} {pid_type} export {rec.pid}:{rec.id}')

            outfile.write(output)
            if not schema:
                rec.pop('$schema', None)
                contributions_sources = current_app.config.get(
                    'RERO_ILS_CONTRIBUTIONS_SOURCES', [])
                for contributions_source in contributions_sources:
                    try:
                        del rec[contributions_source]['$schema']
                    except Exception:
                        pass
            output = ''
            lines = json.dumps(rec, indent=indent).split('\n')
            for line in lines:
                output += f'\n{offset}{line}'
        except Exception as err:
            click.echo(err)
            click.echo(f'ERROR: Can not export pid:{pid}')


def create_personal(
        name, user_id, scopes=None, is_internal=False, access_token=None):
    """Create a personal access token.

    A token that is bound to a specific user and which doesn't expire, i.e.
    similar to the concept of an API key.

    :param name: Client name.
    :param user_id: User ID.
    :param scopes: The list of permitted scopes. (Default: ``None``)
    :param is_internal: If ``True`` it's a internal access token.
            (Default: ``False``)
    :param access_token: personalized access_token.
    :returns: A new access token.
    """
    with db.session.begin_nested():
        scopes = " ".join(scopes) if scopes else ""

        client = Client(
            name=name,
            user_id=user_id,
            is_internal=True,
            is_confidential=False,
            _default_scopes=scopes
        )
        client.gen_salt()

        if not access_token:
            access_token = gen_salt(
                current_app.config.get(
                    'OAUTH2SERVER_TOKEN_PERSONAL_SALT_LEN')
            )
        token = Token(
            client_id=client.client_id,
            user_id=user_id,
            access_token=access_token,
            expires=None,
            _scopes=scopes,
            is_personal=True,
            is_internal=is_internal,
        )

        db.session.add(client)
        db.session.add(token)

    return token


@utils.command('tokens_create')
@click.option('-n', '--name', required=True)
@click.option(
    '-u', '--user', required=True, callback=process_user,
    help='User ID or email.')
@click.option(
    '-s', '--scope', 'scopes', multiple=True, callback=process_scopes)
@click.option('-i', '--internal', is_flag=True)
@click.option(
    '-t', '--access_token', 'access_token', required=False,
    help='personalized access_token.')
@with_appcontext
def tokens_create(name, user, scopes, internal, access_token):
    """Create a personal OAuth token."""
    token = create_personal(
        name, user.id, scopes=scopes, is_internal=internal,
        access_token=access_token)
    db.session.commit()
    click.secho(token.access_token, fg='blue')


@utils.command('add_cover_urls')
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@with_appcontext
def add_cover_urls(verbose):
    """Add cover urls to all documents with isbns."""
    click.secho('Add cover urls.', fg='green')
    search = DocumentsSearch() \
        .filter('term', identifiedBy__type='bf:Isbn') \
        .filter('bool', must_not=[
            Q('term', electronicLocator__content='coverImage')]) \
        .params(preserve_order=True) \
        .sort({'pid': {"order": "asc"}}) \
        .source('pid')
    for idx, hit in enumerate(search.scan()):
        pid = hit.pid
        record = Document.get_record_by_pid(pid)
        url = get_cover_art(record=record, save_cover_url=True)
        if verbose:
            click.echo(f'{idx}:\tdocument: {pid}\t{url}')


@fixtures.command('create_csv')
@click.argument('record_type')
@click.argument('json_file')
@click.argument('output_directory')
@click.option('-l', '--lazy', 'lazy', is_flag=True, default=False)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@click.option('-p', '--create_pid', 'create_pid', is_flag=True, default=False)
@with_appcontext
def create_csv(record_type, json_file, output_directory, lazy, verbose,
               create_pid):
    """Create csv files from json.

    :param verbose: Verbose.
    """
    click.secho(
        f"Create CSV files for: {record_type} from: {json_file}",
        fg='green'
    )

    path = current_jsonschemas.url_to_path(
        get_schema_for_resource(record_type)
    )
    add_schema = get_schema_for_resource(record_type)
    schema = current_jsonschemas.get_schema(path=path)
    schema = _records_state.replace_refs(schema)
    count = 0
    errors_count = 0
    with open(json_file) as infile:
        if lazy:
            # try to lazy read json file (slower, better memory management)
            records = read_json_record(infile)
        else:
            # load everything in memory (faster, bad memory management)
            records = json.load(infile)

        file_name_pidstore = os.path.join(
            output_directory, f'{record_type}_pidstore.csv')
        click.secho(f'\t{file_name_pidstore}', fg='green')
        file_pidstore = open(file_name_pidstore, 'w')
        file_name_metadata = os.path.join(
            output_directory, f'{record_type}_metadata.csv'
        )
        click.secho(f'\t{file_name_metadata}', fg='green')
        file_metadata = open(file_name_metadata, 'w')
        file_name_pids = os.path.join(
            output_directory, f'{record_type}_pids.csv')
        click.secho(f'\t{file_name_pids}', fg='green')
        file_pids = open(file_name_pids, 'w')
        file_name_errors = os.path.join(
            output_directory, f'{record_type}_errors.json')
        file_errors = open(file_name_errors, 'w')
        file_errors.write('[')

        for count, record in enumerate(records, 1):
            pid = record.get('pid')
            if create_pid:
                pid = str(count)
                record['pid'] = pid
            uuid = str(uuid4())
            if verbose:
                click.secho(f'{count}\t{record_type}\t{pid}:{uuid}')
            date = str(datetime.utcnow())
            record['$schema'] = add_schema
            try:
                validate(record, schema)
                file_metadata.write(
                    csv_metadata_line(record, uuid, date)
                )
                file_pidstore.write(
                    csv_pidstore_line(record_type, pid, uuid, date)
                )
                file_pids.write(pid + '\n')
            except Exception as err:
                click.secho(
                    f'{count}\t{record_type}: Error validate in record: ',
                    fg='red')
                click.secho(str(err))
                if errors_count > 0:
                    file_errors.write(',')
                errors_count += 1
                file_errors.write('\n')
                for line in json.dumps(record, indent=2).split('\n'):
                    file_errors.write('  ' + line + '\n')

        file_pidstore.close()
        file_metadata.close()
        file_pids.close()
        file_errors.write('\n]')
        file_errors.close()
    if errors_count == 0:
        os.remove(file_name_errors)
    click.secho(
        f'Created: {count-errors_count} Errors: {errors_count}',
        fg='yellow'
    )


@fixtures.command('bulk_load')
@click.argument('record_type')
@click.argument('csv_metadata_file')
@click.option('-c', '--bulk_count', 'bulkcount', default=0, type=int,
              help='Set the bulk load chunk size.')
@click.option('-r', '--reindex', 'reindex', help='add record to reindex.',
              is_flag=True, default=False)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@with_appcontext
def bulk_load(record_type, csv_metadata_file, bulkcount, reindex, verbose):
    """Agency record management.

    :param csv_metadata_file: metadata: CSV file.
    :param bulk_count: Set the bulk load chunk size.
    :param reindex: add record to reindex.
    :param verbose: Verbose.
    """
    if bulkcount > 0:
        bulk_count = bulkcount
    else:
        bulk_count = current_app.config.get('BULK_CHUNK_COUNT', 100000)

    message = f'Load {record_type} CSV files into database.'
    click.secho(message, fg='green')
    file_name_metadata = csv_metadata_file
    file_name_pidstore = file_name_metadata.replace('metadata', 'pidstore')
    file_name_pids = file_name_metadata.replace('metadata', 'pids')

    record_counts = number_records_in_file(file_name_pidstore, 'csv')
    message = f'  Number of records to load: {record_counts}'
    click.secho(message, fg='green')

    click.secho(f'  Load pids: {file_name_pids}')
    bulk_load_pids(pid_type=record_type, ids=file_name_pids,
                   bulk_count=bulk_count, verbose=verbose)
    click.secho(f'  Load pidstore: {file_name_pidstore}')
    bulk_load_pidstore(pid_type=record_type, pidstore=file_name_pidstore,
                       bulk_count=bulk_count, verbose=verbose)
    click.secho(f'  Load metatada: {file_name_metadata}')
    bulk_load_metadata(pid_type=record_type, metadata=file_name_metadata,
                       bulk_count=bulk_count, verbose=verbose, reindex=reindex)


@fixtures.command('bulk_save')
@click.argument('output_directory')
@click.option('-t', '--pid_types', multiple=True, default=['all'])
@click.option('-d', '--deployment', 'deployment', is_flag=True, default=False)
@click.option('-v', '--verbose', 'verbose', is_flag=True, default=False)
@with_appcontext
def bulk_save(pid_types, output_directory, deployment, verbose):
    """Record dump.

    :param pid_type: Records to export.
        default=//all//
    :param verbose: Verbose.
    """
    file_name_tmp_pidstore = os.path.join(
        output_directory,
        'tmp_pidstore.csv'
    )
    try:
        os.remove(file_name_tmp_pidstore)
    except OSError:
        pass

    all_pid_types = []
    endpoints = current_app.config.get('RECORDS_REST_ENDPOINTS')
    for endpoint in endpoints:
        all_pid_types.append(endpoint)
    if pid_types[0] == 'all':
        pid_types = all_pid_types

    for p_type in pid_types:
        if p_type not in all_pid_types:
            click.secho(
                f'Error {p_type} does not exist!',
                fg='red'
            )
            continue
        # TODO: do we have to save loanid and how we can save it?
        if p_type == 'loanid':
            continue
        click.secho(
            f'Save {p_type} CSV files to directory: {output_directory}',
            fg='green'
        )
        file_prefix = endpoints[p_type].get('search_index')
        if p_type in ['doc', 'hold', 'item', 'count']:
            if deployment:
                file_prefix += '_big'
            else:
                file_prefix += '_small'
        file_name_metadata = os.path.join(
            output_directory,
            f'{file_prefix}_metadata.csv'
        )
        bulk_save_metadata(pid_type=p_type, file_name=file_name_metadata,
                           verbose=verbose)
        file_name_pidstore = os.path.join(
            output_directory,
            f'{file_prefix}_pidstore.csv'
        )
        count = bulk_save_pidstore(pid_type=p_type,
                                   file_name=file_name_pidstore,
                                   file_name_tmp=file_name_tmp_pidstore,
                                   verbose=verbose)

        file_name_pids = os.path.join(
            output_directory,
            f'{file_prefix}_pids.csv'
        )
        bulk_save_pids(pid_type=p_type, file_name=file_name_pids,
                       verbose=verbose)
        click.secho(
            f'Saved records: {count}',
            fg='yellow'
        )
    try:
        os.remove(file_name_tmp_pidstore)
    except OSError:
        pass
