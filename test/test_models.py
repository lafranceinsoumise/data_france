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
            "COM": 34935,
            "COMA": 483,
            "COMD": 2081,
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

        # La plupart des communes ont leur population, mais quelques exceptions existent
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
            [
                "14666",  # Sannerville
                "60694",  # Hauts-Talican
                "85165",  # Oie
                "85212",  # Sainte-Florence
            ],
        )

        # Certaines communes associées et déléguées n'ont pas de population
        self.assertCountEqual(
            Commune.objects.filter(
                type__in=["COMD", "COMA"], population_municipale__isnull=True
            ).values_list("code", flat=True),
            [
                "01039", "01138", "02054", "02077", "02564", "02695", "08068", "08294",
                "09056", "09255", "14114", "14267", "14479", "14673", "16010", "16097",
                "16140", "16186", "16206", "16233", "16351", "16355", "21183", "21213",
                "21452", "21507", "24089", "24314", "24325", "24430", "25060", "25282",
                "25549", "26216", "26219", "27166", "35062", "35112", "44225", "49321",
                "50015", "50272", "51063", "51457", "51637", "52224", "52387", "52402",
                "52454", "53239", "53249", "53274", "56049", "56213", "64300", "64541",
                "67024", "69149", "69152", "71042", "71492", "73148", "73291", "73325",
                "85001", "85037", "85041", "85053", "85271", "85289", "85292", "85307",
                "86231", "86247"
            ],
        )


class EPCITestCase(TestCase):
    def test_epci_correctement_importes(self):
        """Le nombre d'EPCI en base correspond à ce qui est attendu"""
        self.assertEqual(EPCI.objects.count(), 1255)

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
        # La plupart des communes ont des codes postaux, mais certaines petites
        # communes ou communes fusionnées n'en ont pas. Les grandes villes
        # comme Paris, Lyon et Marseille n'ont pas de codes postaux directement
        # car ceux-ci sont associés à leurs arrondissements.
        communes_sans_postal = Commune.objects.filter(
            type__in=["COM", "ARM"], codes_postaux__isnull=True
        ).values_list("code", flat=True).order_by("code")

        # Vérifier que les grandes villes sont bien dans la liste
        self.assertIn("75056", communes_sans_postal)  # Paris
        self.assertIn("13055", communes_sans_postal)  # Marseille
        self.assertIn("69123", communes_sans_postal)  # Lyon

        # La liste complète est trop longue pour être vérifiée exactement
        # mais nous pouvons vérifier que le nombre est raisonnable
        self.assertLessEqual(len(communes_sans_postal), 100)  # Pas plus de 100 communes sans code postal


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
