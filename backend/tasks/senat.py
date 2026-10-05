import csv
import re
import unicodedata

from doit.tools import create_folder

from data_france.typologies import CSP, RelationGroupe
from sources import PREPARE_DIR, SOURCES, SOURCE_DIR
from tasks.cog import DEPARTEMENTS_COG

__all__ = ["task_traiter_senateurs"]


SENAT_DIR = PREPARE_DIR / "senat"
SENATEURS = SENAT_DIR / "senateurs.csv"

# circonscriptions sénatoriales qui ne correspondent pas à un département
CIRCONSCRIPTIONS_HORS_DEPARTEMENTS = {
    "francais etablis hors de france": "99",
    "saint pierre et miquelon": "975",
    "saint barthelemy": "977",
    "saint martin": "978",
    "iles wallis et futuna": "986",
    "polynesie francaise": "987",
    "nouvelle caledonie": "988",
}

SEXES = {"M.": "M", "Mme": "F"}

RELATIONS = {
    "": RelationGroupe.MEMBRE,
    "Apparenté": RelationGroupe.APPARENTE,
    "Rattaché": RelationGroupe.RATTACHE,
}

SANS_GROUPE = "Aucun"
PRESIDENT_GROUPE = "Président"

EMAIL_NON_PUBLIC = "Non public"

# Le Sénat utilise les libellés des CSP, sauf pour certaines professions
# détaillées, rattachées ici à leur CSP
PROFESSIONS_DETAILLEES = {
    "Magistrats": CSP.CIS_CFP,
    "Personnels de direction de la fonction publique": CSP.CIS_CFP,
    "Avocats": CSP.CIS_LIB,
    "Huissiers de justice, officiers ministériels et professions libérales divers": CSP.CIS_LIB,
    "Medecins libéraux généralistes": CSP.CIS_LIB,
    "Pharmaciens libéraux": CSP.CIS_LIB,
    "Vétérinaires (libéraux ou salariés)": CSP.CIS_LIB,
    "Journalistes, secrétaires de rédaction": CSP.CIS_PIAS,
    "Cadres de la publicité; cadres des relations publiques": CSP.CIS_CACE,
}


def task_traiter_senateurs():
    senateurs = SOURCE_DIR / SOURCES.senat.senateurs.filename
    groupes = SOURCE_DIR / SOURCES.senat.groupes.filename

    return {
        "file_dep": [senateurs, groupes, DEPARTEMENTS_COG],
        "targets": [SENATEURS],
        "actions": [
            (create_folder, [SENAT_DIR]),
            (
                traiter_senateurs,
                (senateurs, groupes, DEPARTEMENTS_COG, SENATEURS),
            ),
        ],
    }


def normaliser_libelle(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]+", " ", s.lower()).strip()


def lire_csv_senat(path):
    """Lit un export de data.senat.fr, qui commence par des lignes de
    commentaires préfixées par %"""
    with open(path, encoding=SOURCES.senat.senateurs.encoding, newline="") as f:
        yield from csv.DictReader(l for l in f if not l.startswith("%"))


def normaliser_espaces(s):
    return " ".join(s.replace("''", "'").split())


CODES_CSP = {
    **{normaliser_espaces(label): code for code, label in CSP.choices},
    **PROFESSIONS_DETAILLEES,
}


def code_csp(libelle):
    try:
        return CODES_CSP[normaliser_espaces(libelle)]
    except KeyError:
        raise ValueError(f"Profession inconnue : {libelle!r}")


def nom_groupe(nom, sigle):
    if sigle in nom:
        return nom
    return f"{nom} ({sigle})"


def traiter_senateurs(senateurs_path, groupes_path, departements_path, dest):
    # N.B. : l'export des mandats du Sénat (ODSEN_ELUSEN) n'est plus mis à jour
    # depuis 2023, d'où l'absence de date de début de mandat.
    with open(departements_path, newline="") as f:
        circonscriptions = {
            normaliser_libelle(d[col]): d["DEP"]
            for d in csv.DictReader(f)
            for col in ["NCCENR", "LIBELLE"]
        }
    circonscriptions.update(CIRCONSCRIPTIONS_HORS_DEPARTEMENTS)

    senateurs = [
        s for s in lire_csv_senat(senateurs_path) if s["État"] == "ACTIF"
    ]

    # fonction au sein du groupe, pour les appartenances et fonctions en cours
    appartenances = {
        g["Matricule"]: g
        for g in lire_csv_senat(groupes_path)
        if not g["Date de fin d'appartenance"] and not g["Date de fin de la fonction"]
    }

    # nom complet de chaque groupe à partir de son sigle (l'historique des
    # groupes peut ne pas être renseigné pour certains sénateurs)
    noms_groupes = {
        s["Groupe politique"]: nom_groupe(
            appartenances[s["Matricule"]]["Nom court du groupe politique"],
            s["Groupe politique"],
        )
        for s in senateurs
        if s["Matricule"] in appartenances
    }

    with open(dest, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "code",
                "nom",
                "prenom",
                "sexe",
                "date_naissance",
                "circonscription",
                "groupe",
                "relation",
                "email",
                "profession",
                "description_profession",
            ],
        )
        w.writeheader()

        for s in senateurs:
            sigle = s["Groupe politique"]
            appartenance = appartenances.get(s["Matricule"])
            if sigle == SANS_GROUPE:
                groupe = relation = ""
            else:
                groupe = noms_groupes.get(sigle, sigle)

                if appartenance and appartenance["Nom court fonction"] == PRESIDENT_GROUPE:
                    relation = RelationGroupe.PRESIDENT
                else:
                    relation = RELATIONS[s["Type d'app au grp politique"]]

            email = s["Courrier électronique"]
            if email == EMAIL_NON_PUBLIC:
                email = ""

            w.writerow(
                {
                    "code": s["Matricule"],
                    "nom": s["Nom usuel"],
                    "prenom": s["Prénom usuel"],
                    "sexe": SEXES[s["Qualité"]],
                    "date_naissance": s["Date naissance"][:10],
                    "circonscription": circonscriptions[
                        normaliser_libelle(s["Circonscription"])
                    ],
                    "groupe": groupe,
                    "relation": relation,
                    "email": email,
                    "profession": code_csp(s["PCS INSEE"]),
                    "description_profession": s["Description de la profession"],
                }
            )
