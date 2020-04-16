from django.test import TestCase

from data_france.models import Commune, EPCI, Departement, Region, CodePostal


class CommuneTestCase(TestCase):
    def test_communes_correctement_importees(self):
        """Le nombre de communes au sens large, et de communes au sens propre correspond à ce qui est attendu."""
        attendus = {
            "COM": 34967,
            "COMA": 550,
            "COMD": 2370,
            "ARM": 20 + 9 + 16,
            "SRM": 17 + 9 + 8,
        }

        for type_commune, attendu in attendus.items():
            reel = Commune.objects.filter(type=type_commune).count()
            self.assertEqual(
                reel,
                attendu,
                f"Il devrait y avoir f{attendu} entités de type {type_commune}, il y en a {reel}",
            )

    def test_polygones_disponibles(self):
        """Toutes les communes au sens propre, départements et secteurs ont une géométrie"""
        self.assertFalse(
            Commune.objects.filter(
                type__in=[
                    Commune.TYPE_COMMUNE,
                    Commune.TYPE_ARRONDISSEMENT_PLM,
                    Commune.TYPE_SECTEUR_PLM,
                ],
                geometry__isnull=True,
            ).exists()
        )

    def test_avec_population(self):
        """Les communes ont leur population"""

        # à part Mayotte, toutes les communes et arrondissements ont leur population
        self.assertFalse(
            Commune.objects.filter(
                type__in=[
                    Commune.TYPE_COMMUNE,
                    Commune.TYPE_ARRONDISSEMENT_PLM,
                    Commune.TYPE_SECTEUR_PLM,
                ],
                population_municipale__isnull=True,
            )
            .exclude(departement__code="976")
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

    def test_polygones_disponibles(self):
        self.assertFalse(EPCI.objects.filter(geometry__isnull=True).exists())


class DepartementTestCase(TestCase):
    def test_departements_correctement_importes(self):
        self.assertEqual(Departement.objects.count(), 101)

    def test_communes_attribuees(self):
        self.assertFalse(
            Commune.objects.filter(
                type=Commune.TYPE_COMMUNE, departement__isnull=True
            ).exists()
        )

    def test_seules_les_communes_stricto_sensu_ont_departement(self):
        self.assertFalse(
            Commune.objects.exclude(type=Commune.TYPE_COMMUNE)
            .filter(departement__isnull=False)
            .exists()
        )

    def test_polygones_disponibles(self):
        self.assertFalse(Departement.objects.filter(geometry__isnull=True).exists())


class RegionTestCase(TestCase):
    def test_regions_correctement_importes(self):
        self.assertEqual(Region.objects.count(), 18)

    def test_departements_attribues(self):
        self.assertFalse(Departement.objects.filter(region__isnull=True).exists())

    def test_polygones_disponibles(self):
        self.assertFalse(Region.objects.filter(geometry__isnull=True).exists())


class CodePostalTestCase(TestCase):
    def test_codes_postaux_correctement_importes(self):
        self.assertEqual(CodePostal.objects.count(), 6328)
        # il y a 141 codes postaux qui concernent des collectivités d'outremer qui ne sont
        # pas encore intégrées. Tous sont en 98XXX sauf 3 qui concernent Saint-Pierre-et-Miquelon,
        # Saint-Barthélémy et Saint-Martin
        self.assertEqual(CodePostal.objects.filter(communes__isnull=True).count(), 141)
        self.assertCountEqual(
            CodePostal.objects.filter(communes__isnull=True)
            .exclude(code__startswith="98")
            .values_list("code", flat=True),
            ["97133", "97150", "97500"],
        )

    def test_pas_de_commune_sans_code_postal(self):
        # Les trois seules communes qui n'ont pas de code postal sont
        # Paris, Lyon et Marseille, car les codes postaux sont à la place
        # associés aux arrondissements municipaux.
        self.assertCountEqual(
            Commune.objects.filter(
                type__in=["COM", "ARM"], codes_postaux__isnull=True
            ).values_list("code", flat=True),
            ["75056", "13055", "69123"],
        )
