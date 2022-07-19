import time

from django.core.management.base import BaseCommand
from django.db import connection
from django.db.utils import DatabaseError
from django.utils.translation import ngettext


class Command(BaseCommand):
    """This command waits on for the database to be ready before proceeding"""

    def handle(self, *args, **options):
        wait = 2

        while True:
            try:
                connection.ensure_connection()
                break
            except DatabaseError:
                plural_time = ngettext("second", "seconds", wait)
                self.stdout.write(
                    self.style.WARNING(
                        f"Database unavailable, retrying after {wait} "
                        f"{plural_time}! "
                    )
                )
                time.sleep(wait)
        self.stdout.write(
            self.style.SUCCESS("Database connections successful"))
