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

    def test_avec_population(self):
        """Les communes ont leur population"""

        # à part Mayotte, toutes les communes et arrondissement ont leur population
        self.assertFalse(
            Commune.objects.filter(
                type__in=["COM", "ARM"], population_municipale__isnull=True
            )
            .exclude(code_departement="976")
            .exists()
        )

        # à part ces exceptions récentes, toutes les communes ont leur population municipale
        self.assertCountEqual(
            Commune.objects.filter(
                type__in=["COMD", "COMA"], population_municipale__isnull=True
            ).values_list("code", flat=True),
            ["21183", "21213", "21452", "21507", "44225", "45287", "50649"],
        )


class EPCITestCase(TestCase):
    def test_epci_correctement_importes(self):
        """Le nombre d'EPCI en base correspond à ce qui est attendu"""
        self.assertEqual(EPCI.objects.count(), 1259)

    def test_epci_associees_correctement(self):
        """Seules quatre communes insulaires ne font pas partie d'une intercommunalité"""
        self.assertCountEqual(
            Commune.objects.filter(
                type=Commune.TYPE_COMMUNE, epci__isnull=True
            ).values_list("nom", flat=True),
            ["Île-de-Bréhat", "Île-de-Sein", "Ouessant", "Île-d'Yeu"],
        )

    def test_seules_communes_ont_epci(self):
        """Seules les communes au sens propre doivent être associées à une intercommunalité"""
        self.assertFalse(
            Commune.objects.exclude(type=Commune.TYPE_COMMUNE)
            .filter(epci__isnull=False)
            .exists()
        )
