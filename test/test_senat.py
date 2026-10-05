import csv
import sys
import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from data_france.typologies import CSP, RelationGroupe

sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from tasks.senat import code_csp, nom_groupe, normaliser_libelle, traiter_senateurs


ENTETE_SENATEURS = [
    "Matricule",
    "Qualité",
    "Nom usuel",
    "Prénom usuel",
    "État",
    "Date naissance",
    "Date de décès",
    "Groupe politique",
    "Type d'app au grp politique",
    "Commission permanente",
    "Circonscription",
    "Fonction au Bureau du Sénat",
    "Courrier électronique",
    "PCS INSEE",
    "Catégorie professionnelle",
    "Description de la profession",
]

ENTETE_GROUPES = [
    "Matricule",
    "Id appartenance",
    "Id fonction occupée",
    "Nom",
    "Prénom",
    "Code du groupe politique",
    "Nom court du groupe politique",
    "Date de début d'appartenance",
    "Date de fin d'appartenance",
    "Nom court fonction",
    "Date de début de la fonction",
    "Date de fin de la fonction",
]

DEPARTEMENTS = [
    ["DEP", "REG", "CHEFLIEU", "TNCC", "NCC", "NCCENR", "LIBELLE"],
    ["04", "93", "04070", "1", "ALPES DE HAUTE PROVENCE", "Alpes-de-Haute-Provence", "Alpes-de-Haute-Provence"],
    ["92", "11", "92050", "4", "HAUTS DE SEINE", "Hauts-de-Seine", "Hauts-de-Seine"],
    ["974", "04", "97411", "0", "LA REUNION", "Réunion", "La Réunion"],
]


def senateur(matricule, circonscription, groupe, **kwargs):
    return {
        "Matricule": matricule,
        "Qualité": "M.",
        "Nom usuel": f"Nom{matricule}",
        "Prénom usuel": "Prénom",
        "État": "ACTIF",
        "Date naissance": "1960-01-31 00:00:00.0",
        "Groupe politique": groupe,
        "Type d'app au grp politique": "",
        "Commission permanente": "Aucune",
        "Circonscription": circonscription,
        "Courrier électronique": f"{matricule}@senat.fr",
        "PCS INSEE": "Cadres de la fonction publique",
        "Description de la profession": "",
        **kwargs,
    }


def appartenance(matricule, nom_groupe, fonction="Membre", fin=""):
    return {
        "Matricule": matricule,
        "Nom court du groupe politique": nom_groupe,
        "Date de début d'appartenance": "2023-10-02 00:00:00.0",
        "Date de fin d'appartenance": fin,
        "Nom court fonction": fonction,
        "Date de fin de la fonction": fin,
    }


SENATEURS = [
    senateur(
        "1A",
        "Hauts-de-Seine",
        "Les Républicains",
        **{"Qualité": "Mme", "Description de la profession": "Haute fonctionnaire"},
    ),
    senateur(
        "2B",
        "Alpes de Haute-Provence",
        "SER",
        **{"Courrier électronique": "Non public", "PCS INSEE": "Avocats"},
    ),
    senateur("3C", "Français établis hors de France", "Aucun"),
    senateur(
        "4D",
        "Saint-Martin",
        "RDPI",
        **{
            "Type d'app au grp politique": "Apparenté",
            "PCS INSEE": "Chefs d''entreprise de 10 salariés ou plus",
        },
    ),
    senateur(
        "5E",
        "La Réunion",
        "RDPI",
        **{
            "Type d'app au grp politique": "Rattaché",
            "PCS INSEE": "Personnes diverses sans activité  professionnelle de moins de 60 ans (sauf retraités)",
        },
    ),
    senateur("6F", "Hauts-de-Seine", "UC", **{"État": "ANCIEN"}),
]

GROUPES = [
    appartenance("1A", "Groupe UMP", fin="2015-05-30 00:00:00.0"),
    appartenance("1A", "Groupe Les Républicains"),
    appartenance("2B", "Groupe Socialiste, Écologiste et Républicain", "Président"),
    appartenance("3C", "Sénateurs n'appartenant à aucun groupe"),
    # pas d'appartenance en cours pour 4D
    appartenance(
        "5E", "Groupe Rassemblement des démocrates, progressistes et indépendants"
    ),
]


def ecrire_export_senat(path, entete, lignes):
    with open(path, "w", encoding="latin1", newline="") as f:
        f.write("% Requête : select ...\r\n% Aucun paramètre\r\n")
        w = csv.DictWriter(f, fieldnames=entete, restval="", lineterminator="\r\n")
        w.writeheader()
        w.writerows(lignes)


class TraiterSenateursTest(SimpleTestCase):
    def traiter(self, senateurs=SENATEURS, groupes=GROUPES):
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            ecrire_export_senat(d / "senateurs.csv", ENTETE_SENATEURS, senateurs)
            ecrire_export_senat(d / "groupes.csv", ENTETE_GROUPES, groupes)
            with open(d / "departements.csv", "w", newline="") as f:
                csv.writer(f).writerows(DEPARTEMENTS)

            traiter_senateurs(
                d / "senateurs.csv",
                d / "groupes.csv",
                d / "departements.csv",
                d / "resultat.csv",
            )

            with open(d / "resultat.csv", newline="") as f:
                return {s["code"]: s for s in csv.DictReader(f)}

    def test_seuls_senateurs_actifs(self):
        self.assertEqual(set(self.traiter()), {"1A", "2B", "3C", "4D", "5E"})

    def test_identite(self):
        s = self.traiter()["1A"]
        self.assertEqual(s["nom"], "Nom1A")
        self.assertEqual(s["prenom"], "Prénom")
        self.assertEqual(s["sexe"], "F")
        self.assertEqual(s["date_naissance"], "1960-01-31")
        self.assertEqual(self.traiter()["2B"]["sexe"], "M")

    def test_circonscriptions(self):
        res = self.traiter()
        self.assertEqual(
            {code: s["circonscription"] for code, s in res.items()},
            {"1A": "92", "2B": "04", "3C": "99", "4D": "978", "5E": "974"},
        )

    def test_circonscription_inconnue(self):
        with self.assertRaises(KeyError):
            self.traiter(senateurs=[senateur("1A", "Atlantide", "UC")])

    def test_groupes(self):
        res = self.traiter()
        self.assertEqual(res["1A"]["groupe"], "Groupe Les Républicains")
        self.assertEqual(
            res["2B"]["groupe"], "Groupe Socialiste, Écologiste et Républicain (SER)"
        )
        self.assertEqual(res["3C"]["groupe"], "")
        # 4D n'a pas d'appartenance en cours : le nom du groupe est retrouvé
        # grâce à 5E, qui a le même sigle
        nom_rdpi = (
            "Groupe Rassemblement des démocrates, progressistes et indépendants (RDPI)"
        )
        self.assertEqual(res["4D"]["groupe"], nom_rdpi)
        self.assertEqual(res["5E"]["groupe"], nom_rdpi)

    def test_groupe_sans_historique(self):
        res = self.traiter(groupes=[])
        self.assertEqual(res["4D"]["groupe"], "RDPI")

    def test_relations(self):
        res = self.traiter()
        self.assertEqual(
            {code: s["relation"] for code, s in res.items()},
            {
                "1A": RelationGroupe.MEMBRE,
                "2B": RelationGroupe.PRESIDENT,
                "3C": "",
                "4D": RelationGroupe.APPARENTE,
                "5E": RelationGroupe.RATTACHE,
            },
        )

    def test_email(self):
        res = self.traiter()
        self.assertEqual(res["1A"]["email"], "1A@senat.fr")
        self.assertEqual(res["2B"]["email"], "")

    def test_profession(self):
        res = self.traiter()
        self.assertEqual(res["1A"]["profession"], str(CSP.CIS_CFP.value))
        self.assertEqual(res["2B"]["profession"], str(CSP.CIS_LIB.value))
        self.assertEqual(res["4D"]["profession"], str(CSP.ACC_CHE.value))
        self.assertEqual(res["1A"]["description_profession"], "Haute fonctionnaire")


class FonctionsSenatTest(SimpleTestCase):
    def test_normaliser_libelle(self):
        self.assertEqual(
            normaliser_libelle("Alpes de Haute-Provence"),
            normaliser_libelle("Alpes-de-Haute-Provence"),
        )
        self.assertEqual(
            normaliser_libelle("Français établis hors de France"),
            "francais etablis hors de france",
        )

    def test_code_csp(self):
        self.assertEqual(code_csp("Professions libérales et assimilés"), CSP.CIS_LIB)
        # espaces multiples et apostrophes doublées de l'export du Sénat
        self.assertEqual(
            code_csp("Chefs d''entreprise de 10 salariés ou plus"), CSP.ACC_CHE
        )
        self.assertEqual(code_csp("Professeurs,  professions scientifiques"), CSP.CIS_PPS)
        # profession détaillée, rattachée à sa CSP
        self.assertEqual(code_csp("Magistrats"), CSP.CIS_CFP)

    def test_code_csp_inconnu(self):
        with self.assertRaises(ValueError):
            code_csp("Astronaute")

    def test_nom_groupe(self):
        self.assertEqual(nom_groupe("Groupe Union Centriste", "UC"), "Groupe Union Centriste (UC)")
        self.assertEqual(nom_groupe("Groupe du RDSE", "RDSE"), "Groupe du RDSE")
