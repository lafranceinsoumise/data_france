from django.test import TestCase

from data_france.models import Commune, EPCI


class CommuneTestCase(TestCase):
    def test_communes_correctement_importees(self):
        """Le nombre de communes au sens large, et de communes au sens propre correspond à ce qui est attendu."""
        self.assertEqual(Commune.objects.count(), 37932)
        self.assertEqual(
            Commune.objects.filter(type=Commune.TYPE_COMMUNE).count(), 34967
        )

    def test_polygones_disponibles(self):
        """Toutes les communes au sens propre ont une géométrie"""
        self.assertFalse(
            Commune.objects.filter(
                type=Commune.TYPE_COMMUNE, geometry__isnull=True
            ).exists()
        )


class EPCITestCase(TestCase):
    def test_epci_correctement_importes(self):
        """Le nombre d'EPCI en base correspond à ce qui est attendu"""
        self.assertEqual(EPCI.objects.count(), 1259)

    def test_epci_associees_correctement(self):
        self.assertCountEqual(
            Commune.objects.filter(
                type=Commune.TYPE_COMMUNE, epci__isnull=True
            ).values_list("nom", flat=True),
            ["Île-de-Bréhat", "Île-de-Sein", "Ouessant", "Île-d'Yeu"],
        )

    def test_seules_communes_ont_epci(self):
        self.assertFalse(
            Commune.objects.exclude(type=Commune.TYPE_COMMUNE)
            .filter(epci__isnull=False)
            .exists()
        )
