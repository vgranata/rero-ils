# -*- coding: utf-8 -*-
#
# RERO ILS
# Copyright (C) 2021 RERO
# Copyright (C) 2021 UCLouvain
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

"""API for manipulating "availability" circulation notifications."""

from __future__ import absolute_import, print_function

from datetime import datetime, timedelta, timezone

from rero_ils.modules.documents.dumpers import DocumentNotificationDumper
from rero_ils.modules.libraries.dumpers import \
    LibraryCirculationNotificationDumper
from rero_ils.modules.libraries.exceptions import LibraryNeverOpen
from rero_ils.modules.patrons.dumpers import PatronNotificationDumper

from .circulation import CirculationNotification
from ..models import NotificationChannel, NotificationType


class AvailabilityCirculationNotification(CirculationNotification):
    """Availability circulation notifications class."""

    def can_be_cancelled(self):
        """Check if a notification can be be canceled.

        An AVAILABILITY notification can be cancelled if the related item
        is already ON_LOAN. We need to call the loan to check all notification
        candidates and check if AVAILABILITY is into candidates.

        :return a tuple with two values: a boolean to know if the notification
                can be cancelled; the reason why the notification can be
                cancelled (only present if tuple first value is True).
        """
        # Check if parent class would cancel the notification. If yes other
        # check could be skipped.
        can, reason = super().can_be_cancelled()
        if can:
            return can, reason
        # Check loan notification candidate (by unpacking tuple's notification
        # candidate)
        candidates_types = [
            n[1] for n in
            self.loan.get_notification_candidates(trigger=None)
        ]
        if self.type not in candidates_types:
            msg = "Notification type isn't into notification candidate"
            return True, msg
        # we don't find any reasons to cancel this notification
        return False, None

    @classmethod
    def _availability_end_date(cls, library):
        """Get the date until when a document will be available.

        :param library: the library where to search the end date.
        :return the formatted availability end date.
        """
        # TODO: make availability days variable (now fixed to 10 days)
        keep_until = datetime.now(timezone.utc) + timedelta(days=10)
        try:
            return library.next_open(keep_until).strftime("%d.%m.%Y")
        except LibraryNeverOpen:
            return 'never open'

    @classmethod
    def get_notification_context(cls, notifications=None):
        """Get the context to render the notification template.

        AVAILABILITY notification are always aggregated by library and by
        patron. So we could use the first notification of the list to get
        global information about these data. We need to loop on each
        notification to get the documents information and pickup location
        informations related to each loan.
        """
        context = {}
        notifications = notifications or []
        if not notifications:
            return context

        patron = notifications[0].patron
        library = notifications[0].pickup_library
        include_address = notifications[0].get_communication_channel() == \
            NotificationChannel.MAIL
        # Dump basic informations
        context.update({
            'include_patron_address': include_address,
            'patron': patron.dumps(dumper=PatronNotificationDumper()),
            'library': library.dumps(
                dumper=LibraryCirculationNotificationDumper()),
            'loans': [],
            'delay': 0
        })
        # Availability notification could be sent with a delay. We need to find
        # this delay into the library notifications settings.
        for setting in library.get('notification_settings', []):
            if setting['type'] == NotificationType.AVAILABILITY:
                context.update({'delay': setting.get('delay', 0)})
        # Add metadata for any ``notification.loan`` of the notifications list
        doc_dumper = DocumentNotificationDumper()
        for notification in notifications:
            loc = lib = None
            if notification.pickup_location:
                loc = notification.pickup_location
                lib = notification.pickup_library
            elif notification.transaction_location:
                loc = notification.transaction_location
                lib = notification.transaction_library

            if loc and lib:
                context['loans'].append({
                    'document': notification.document.dumps(dumper=doc_dumper),
                    'pickup_name': loc.get('pickup_name', lib.get('name')),
                    'pickup_until': cls._availability_end_date(lib)
                })
        return context
