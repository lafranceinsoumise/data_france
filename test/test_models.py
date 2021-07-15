from django.test import TestCase

from data_france.models import (
    Commune,
    EPCI,
    Departement,
    Region,
    CodePostal,
    CollectiviteDepartementale,
    CollectiviteRegionale,
    CirconscriptionConsulaire,
    CirconscriptionLegislative,
)


class CommuneTestCase(TestCase):
    def test_communes_correctement_importees(self):
        """Le nombre de communes au sens large, et de communes au sens propre correspond à ce qui est attendu."""
        attendus = {
            "COM": 34965,
            "COMA": 517,
            "COMD": 2215,
            "ARM": 20 + 9 + 16,
            "SRM": 17 + 9 + 8,
        }

        for type_commune, attendu in attendus.items():
            reel = Commune.objects.filter(type=type_commune).count()
            self.assertEqual(
                reel,
                attendu,
                f"Il devrait y avoir {attendu} entités de type {type_commune}, il y en a {reel}",
            )

    def test_polygones_disponibles(self):
        """Toutes les communes au sens propre, départements et secteurs ont une géométrie"""
        self.assertFalse(
            Commune.objects.filter(
                type__in=[
                    Commune.TypeCommune.COMMUNE,
                    Commune.TypeCommune.ARRONDISSEMENT_PLM,
                    Commune.TypeCommune.SECTEUR_PLM,
                ],
                geometry__isnull=True,
            ).exists()
        )

    def test_avec_population(self):
        """Les communes ont leur population"""

        # à part Mayotte, toutes les communes et arrondissements ont leur population
        self.assertCountEqual(
            Commune.objects.filter(
                type__in=[
                    Commune.TypeCommune.COMMUNE,
                    Commune.TypeCommune.ARRONDISSEMENT_PLM,
                    Commune.TypeCommune.SECTEUR_PLM,
                ],
                population_municipale__isnull=True,
            )
            .exclude(code__startswith="976")  # il manque toutes les communes de Mayotte
            .values_list("code", flat=True),
            [],
        )

        # deux de ces exceptions sont des communes qui ont changé de département
        # en fusionnant, toutes les autres sauf une sont des communes associées
        # devenues déléguées. Aucune idée des raisons de l'absence pour la dernière (45287)
        self.assertCountEqual(
            Commune.objects.filter(
                type__in=["COMD", "COMA"], population_municipale__isnull=True
            ).values_list("code", flat=True),
            [
                "08068",
                "14114",
                "14267",
                "14479",
                "14673",
                "44225",
                "52224",
                "52387",
                "52402",
                "52454",
                "73148",
                "73291",
                "73325",
            ],
        )


class EPCITestCase(TestCase):
    def test_epci_correctement_importes(self):
        """Le nombre d'EPCI en base correspond à ce qui est attendu"""
        self.assertEqual(EPCI.objects.count(), 1254)

    def test_epci_associees_correctement(self):
        """Seules quatre communes insulaires ne font pas partie d'une intercommunalité"""
        self.assertCountEqual(
            Commune.objects.filter(
                type=Commune.TypeCommune.COMMUNE, epci__isnull=True
            ).values_list("nom", flat=True),
            ["Île-de-Bréhat", "Île-de-Sein", "Ouessant", "Île-d'Yeu"],
        )

    def test_seules_communes_ont_epci(self):
        """Seules les communes au sens propre doivent être associées à une intercommunalité"""
        self.assertFalse(
            Commune.objects.exclude(type=Commune.TypeCommune.COMMUNE)
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
                type=Commune.TypeCommune.COMMUNE, departement__isnull=True
            ).exists()
        )

    def test_seules_les_communes_stricto_sensu_ont_departement(self):
        self.assertFalse(
            Commune.objects.exclude(type=Commune.TypeCommune.COMMUNE)
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


class CollectiviteDepartementaleTest(TestCase):
    def test_import_correct(self):
        # 96 départements en métropole. 5 départements d'outremer, soit un total
        # de 101 départements.
        #
        # Trois cas particuliers : une unique collectivité pour l'Alsace, une
        # unique collectivité pour la Corse, et une collectivité supplémentaire
        # avec la Métropole de Lyon.
        #
        # A noter que deux départements d'outremer ont une collectivité unique,
        # et que la ville de Paris a les compétences départementales sur le
        # territoire du département de Paris, mais ça ne change pas le nombre de
        # collectivités.
        #
        # Le total est donc de 101 - 2 + 1 = 100
        self.assertEqual(CollectiviteDepartementale.objects.count(), 100)


class CollectiviteRegionaleTest(TestCase):
    def test_import_correct(self):
        self.assertEqual(CollectiviteRegionale.objects.count(), 18)


class CirconscriptionConsulaireTest(TestCase):
    def test_import_correct(self):
        self.assertEqual(CirconscriptionConsulaire.objects.count(), 130)


class CirconscriptionLegislativeTest(TestCase):
    def test_import_correct(self):
        self.assertEqual(CirconscriptionLegislative.objects.count(), 577)
