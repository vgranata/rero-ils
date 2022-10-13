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

"""Permissions for statistics configuration."""

from rero_ils.modules.permissions import RecordPermission
from rero_ils.modules.patrons.api import current_librarian
from rero_ils.permissions import librarian_permission

class StatCfgPermission(RecordPermission):
    """Stat configuration permissions."""

    @classmethod
    def list(cls, user, record=None):
        """List permission check.

        :param user: Logged user.
        :param record: Record to check.
        :return: True is action can be done.
        """
        # Operation allowed only for system librarians
        # TODO filter records by org_pid
        if librarian_permission.require().can():
            if current_librarian.is_system_librarian:
                return True
        return False

    @classmethod
    def read(cls, user, record):
        """Read permission check.

        :param user: Logged user.
        :param record: Record to check.
        :return: True is action can be done.
        """
        if librarian_permission.require().can():
            if not current_librarian.is_system_librarian:
                return False
            org_pid = current_librarian.organisation_pid
            if record['org_pid'] == org_pid:
                return True
        return False
        

    @classmethod
    def create(cls, user, record=None):
        """Create permission check.

        :param user: Logged user.
        :param record: Record to check.
        :return: True is action can be done.
        """
        if librarian_permission.require().can():
            if current_librarian.is_system_librarian:
                return True
        return False

    @classmethod
    def update(cls, user, record):
        """Update permission check.

        :param user: Logged user.
        :param record: Record to check.
        :return: True is action can be done.
        """
        return cls.read(user, record)

    @classmethod
    def delete(cls, user, record):
        """Delete permission check.

        :param user: Logged user.
        :param record: Record to check.
        :return: True if action can be done.
        """
        return cls.read(user, record)
