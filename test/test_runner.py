from django.test.runner import DiscoverRunner

from data_france import data


class TestRunner(DiscoverRunner):
    def setup_databases(self, **kwargs):
        old_names = super().setup_databases()
        data.importer_donnees()
        return old_names
