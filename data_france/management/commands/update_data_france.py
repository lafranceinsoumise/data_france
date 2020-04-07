from django.core.management import BaseCommand

from data_france.data import importer_donnees


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-u", "--using")

    def handle(self, *args, using, **options):
        importer_donnees(using=using)
