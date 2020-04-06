from django.core.management import BaseCommand

from data_france import data


class Command(BaseCommand):
    def handle(self, *args, **options):
        self.stderr.write("Chargement des EPCI")
        data.importer_epci()

        self.stderr.write("Chargement des communes")
        data.importer_communes()
