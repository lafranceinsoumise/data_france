import csv
import json
import re
import sys
import tarfile

import pandas as pd
from doit.tools import create_folder
from lxml import etree
from shapely.geometry import Point

from data_france.data import VILLES_PLM
from data_france.utils import TypeNom
from sources import SOURCES, PREPARE_DIR, SOURCE_DIR
from tasks.cog import CORR_SOUS_COMMUNES, COMMUNE_TYPE_ORDERING
from utils import normaliser_nom


__all__ = [
    "task_extraire_mairies",
    "task_extraire_conseils_departementaux",
    "task_post_traitement_mairies",
]


MAIRIE_RE = re.compile(r"\d[\dAB]\d?/mairie-\d[\dAB]\d{3}-\d{2}.xml$")
CONSEIL_DEPARTEMENTAL_RE = re.compile(r"\d[\dAB]\d?/cg-\d[\dAB]\d{3}-\d{2}.xml$")

DEFAUT_EDITEUR = (
    "La Direction de l'information légale et administrative (Premier ministre)"
)

ANNUAIRE_SOURCE = SOURCES.premier_ministre.annuaire_administration
ANNUAIRE_DIR = PREPARE_DIR / ANNUAIRE_SOURCE.path
ANNUAIRE_FILE = ANNUAIRE_DIR / "2024-05-31_060016-data.gouv_local.json"
MAIRIES_EXTRAITES = ANNUAIRE_DIR / "mairies.ndjson"
MAIRIES_TRAITEES = ANNUAIRE_DIR / "mairies.csv"
CONSEILS_DEPARTEMENTAUX_EXTRAITS = ANNUAIRE_DIR / "conseils_departementaux.ndjson"


def task_extraire_mairies():
    return {
        "file_dep": [ANNUAIRE_FILE],
        "targets": [MAIRIES_EXTRAITES],
        "actions": [
            (create_folder, [ANNUAIRE_DIR]),
            (extraire_organismes, (ANNUAIRE_FILE, MAIRIES_EXTRAITES, MAIRIE_RE)),
        ],
    }


def task_extraire_conseils_departementaux():
    return {
        "file_dep": [ANNUAIRE_FILE],
        "targets": [CONSEILS_DEPARTEMENTAUX_EXTRAITS],
        "actions": [
            (create_folder, [ANNUAIRE_DIR]),
            (
                extraire_organismes,
                (
                    ANNUAIRE_FILE,
                    CONSEILS_DEPARTEMENTAUX_EXTRAITS,
                    CONSEIL_DEPARTEMENTAL_RE,
                ),
            ),
        ],
    }


def task_post_traitement_mairies():
    return {
        "file_dep": [MAIRIES_EXTRAITES, CORR_SOUS_COMMUNES],
        "targets": [MAIRIES_TRAITEES],
        "actions": [
            (
                post_traitement_mairies,
                (MAIRIES_EXTRAITES, CORR_SOUS_COMMUNES, MAIRIES_TRAITEES),
            )
        ],
    }


def extraire_organismes(source_path, dest_path, path_regex):
    with open(source_path, "r") as source, open(dest_path, "w") as dest:
        data = json.load(source)
        types = set()
        for organisme in data["service"]:
            if not organisme["nom"].startswith("Mairie"):
                continue

            types.add(organisme["categorie"])
            json.dump(extraire_organisme(organisme), dest, indent=None)
            dest.write("\n")




def extraire_organisme(organisme):
    res = {
        "id": organisme["id"],
        "code": organisme["code_insee_commune"],
        "Nom": organisme["nom"],
        "Adresse": extraire_adresse(organisme),
        "Contact": {
            "Téléphone": organisme["telephone"],
            "Email": organisme["adresse_courriel"],
            "Url": organisme["site_internet"],
        },
        "Ouvert": "",
        "Editeur": "",
        "Commentaire": "",
    }

    return res


def extraire_adresse(data):
    if not data["adresse"]:
        return None

    adresse = data["adresse"][0]
    res = {
        "Lignes": [
            adresse["numero_voie"],
            adresse["complement1"],
            adresse["complement2"],
        ],
        "NomCommune": adresse["nom_commune"],
        "CodePostal": adresse["code_postal"],
        "Accessibilité": {
            "type": adresse["accessibilite"],
            "détail": adresse["accessibilite"],
        },
    }

    if adresse["longitude"] and adresse["latitude"]:
        res["Localisation"] = [
            adresse["longitude"] and float(adresse["longitude"]),
            adresse["latitude"] and float(adresse["latitude"]),
        ]

    return res


# Les cas d'erreurs concernés sont les suivants :
# - CHAR : le libellé de mairie utilise la charnière, ce qui empêche la mise en correspondance
# - COM : même si le numéro de mairie n'est pas 01, il s'agit bien de la mairie d'une COM
# - LIBCOM : le libellé indique incorrectement qu'il s'agit de la mairie de la commune nouvelle et non
#   de la commune déléguée
EXCEPTIONS = {
    "mairie-37232-02": ("COMD", "37227"),  # Saint-Michel-sur-Loire (CHAR)
    "mairie-37232-03": ("COMD", "37120"),  # Ingrandes de Touraine (CHAR)
    "mairie-56033-02": ("COMD", "56183"),  # Quelneuc (LIBCOM),
    "mairie-72033-02": ("COM", "72033"),  # (COM)
    "mairie-79078-04": ("COMD", "79247"),  # Saint-Étienne-la-Cigogne (LIBCOM)
    "mairie-79136-02": ("COMD", "79006"),  # Les Alleuds (LIBCOM)
    "mairie-38446-02": ("COM", "38446"),  # Saint-Pierre-d'Entremont (COM)
    "mairie-39483-02": ("COM", "39483"),  # Saint-Hymetière sur Valouse
    "mairie-14712-02": ("COM", "14666"),  # Sannerville, ville ressuscitée
    "mairie-27055-02": ("COM", "27055"),  # Bérengeville-la-Campagne (COM)
    "mairie-35072-04": ("COM", "35072"),  # Châtillon-en-Vendelay
    "mairie-28103-10": ("COM", "28103"),  # Cloyes-les-trois-rivieres
    "mairie-28150-01": ("COMD", "28150"),  # Ferté-Villeneuil
    "mairie-28262-01": ("COMD", "28262"),  # Montigny-le-Gannelon
    "mairie-28340-01": ("COMD", "28340"),  # Saint-Hilaire-sur-Yerre
    "mairie-28103-01": ("COMD", "28103"),  # Charray
    "mairie-49044-01": ("COMD", "49044"),  # Breil
    "mairie-49122-01": ("COMD", "49122"),  # Dénezé-sous-le-Lude
    "mairie-49175-01": ("COMD", "49175"),  # Linières-Bouton
    "mairie-49062-01": ("COMD", "49062"),  # Chalonnes-sous-le-Lude
    "mairie-50534-01": ("COMD", "50534"),  # Saint-Pelerin
}


# Il s'agit des communes dont l'existence n'a pas été préservée comme commune déléguée suite à la fusion
COMMUNE_NON_CONSERVEE = {
    "mairie-65399-02",  # Vizos
    "mairie-65081-02",  # Molère
    "mairie-44160-02",  # Saint-Géréon
    "mairie-33008-02",  # Cantois
    "mairie-33268-02",  # Cantenac
    "mairie-79251-02",  # Pouffonds
    "mairie-62210-02",  # Canteleux
    "mairie-88475-02",  # Rocourt
    "mairie-39016-02",  # Chisséria
    "mairie-39290-02",  # Chatonnay
    "mairie-39290-03",  # Fétigny
    "mairie-39290-04",  # Savigna
    "mairie-39483-01",  # Chemilla
    "mairie-39483-03",  # Cézia
    "mairie-39483-04",  # Lavans-sur-Valouse
    "mairie-70491-02",  # Motey-sur-Saône
    "mairie-17160-02",  # Saint-Romain-sur-Gironde
    "mairie-30339-02",  # Notre-Dame-de-la-Rouvière
    "mairie-24142-02",  # Mouzens
    "mairie-24087-02",  # Bèzenac
    "mairie-24534-02",  # Flaugeac
    "mairie-77109-02",  # Cucharnoy
    "mairie-85194-02",  # Château-d'Olonne
    "mairie-85194-03",  # Olonne-sur-Mer
    "mairie-14126-02",  # Saint-Laurent-Du-Mont
    "mairie-14713-02",  # Goupillières
    "mairie-28127-02",  # Bullou
    "mairie-05039-02",  # Saint-Eusèbe-en-Champsaur
    "mairie-05039-03",  # Costes
    "mairie-80442-02",  # Grécourt
    "mairie-25222-02",  # Charbonnières-les-Sapins
    "mairie-25368-02",  # Chaudefontaine
    "mairie-25334-02",  # Labergement-du-Navois
    "mairie-25222-03",  # Verrières du Grosbois
    "mairie-25460-02",  # Montfort
    "mairie-25245-02",  # Arguel
    "mairie-25558-02",  # Foucherans
    "mairie-50535-02",  # Sainte-Pience
    "mairie-50597-02",  # La Gohannière
    "mairie-95040-02",  # Gadancourt
    "mairie-63335-02",  # Creste
    "mairie-34154-02",  # Carnon
    "mairie-31277-02",  # Pradère-les-Bourguets
    "mairie-25156-02",  # Santoche
    "mairie-18222-02",  # Saint-Lunaise
    "mairie-45287-02",  # Saint-Loup-de-Gonois
    "mairie-56173-02",  # Bieuzy
    "mairie-56138-02",  # Monterrein
    "mairie-72189-02",  # Dissé-sous-Ballon
    "mairie-23192-02",  # Une des deux anciennes COMD de Fursac
    "mairie-12090-02",  # Balsac
    "mairie-22251-02",  # Tréméloir
    "mairie-38293-02",  # Panissage
    "mairie-16230-05",  # Saint-Eutrope
    "mairie-16046-02",  # Cressac-Saint-Genis
    "mairie-16023-02",  # Bayers
    "mairie-16230-02",  # Aignes-et-Puypéroux
    "mairie-16230-03",  # Montmoreau (COMD)
    "mairie-16230-04",  # Saint-Laurent-de-Belzagot
    "mairie-39378-02",  # Saint-Jean-d'Étreux
    "mairie-73227-02",  # La Perrière
    "mairie-14406-03",  # Cully
    "mairie-14200-02",  # Saint-Gabriel-Brécy
    "mairie-14355-02",  # Amblie
    "mairie-14200-03",  # Villiers-le-Sec
    "mairie-14355-03",  # Tierceville
    "mairie-28012-04",  # Courtalain
    "mairie-28012-03",  # Châtillon-en-Dunois
    "mairie-28012-06",  # Saint-Pellerin
    "mairie-28012-05",  # Langey
    "mairie-28012-02",  # Boisgasson
    "mairie-05101-02",  # Vallouise
    "mairie-27676-03",  # Tosny
    "mairie-27588-03",  # Saint-Pierre d'Autils
    "mairie-27290-02",  # Tilleul-Othon
    "mairie-27554-02",  # Saint-Just
    "mairie-50592-02",  # Pont-Farcy
    "mairie-50139-02",  # Troisgots
    "mairie-50417-02",  # Morsalines
    "mairie-01227-02",  # Saint-Champ
    "mairie-36093-02",  # Saint-Pierre-de-Lamps
    "mairie-91228-02",  # Courcouronnes
    "mairie-62431-01",  # Herbelles
    "mairie-67439-01",  # Schaffhouse-sur-Zorn
}


def _nom_ville_from_nom_mairie(nom):
    parties = nom.split(" - ", 1)
    if len(parties) > 1:
        return parties[1]
    parties = nom.split(" – ", 1)
    if len(parties) > 1:
        return parties[1]

    return nom


def _commune_key(c):
    return (COMMUNE_TYPE_ORDERING.index(c[0]), c[1])


def obtenir_commune_matcher(corr_sous_communes):
    # le fichier de correspondances qui liste toutes les sous-communes et leurs
    # communes parentes
    corr_sous_communes = pd.read_csv(
        corr_sous_communes, dtype={"code": str, "commune_parent": str}
    )

    corr_sous = {
        **{
            (parent, normaliser_nom(f"{TypeNom(typ_nom).article}{nom}")): (typ, code)
            for typ, code, typ_nom, nom, parent in corr_sous_communes.itertuples(
                index=False
            )
        },
        **{
            (parent, normaliser_nom(nom)): (typ, code)
            for typ, code, _, nom, parent in corr_sous_communes.itertuples(index=False)
        },
    }

    # cette table de correspondance directe ne peut être utilisée que pour les
    # cas où aucune commune normale n'existe avec le même numéro.
    corr_direct = {
        code: type for type, code, *_ in corr_sous_communes.itertuples(index=False)
    }

    corr_plm = {
        f"{code_arr}": [
            ("ARM", code_arr),
            ("SRM", secteur.code),
        ]
        for ville in VILLES_PLM
        for secteur in ville.secteurs
        for code_arr in secteur.arrondissements
    }

    # À Paris, c'est l'ancienne mairie du 3ème qui est maintenant mairie du 1er secteur
    # on supprime la mention de secteur des trois autres secteurs
    corr_plm["75101"].pop()
    corr_plm["75102"].pop()
    corr_plm["75104"].pop()

    def commune_matcher(mairie):
        if mairie["id"] in EXCEPTIONS:
            return [EXCEPTIONS[mairie["id"]]]

        if mairie["id"] in COMMUNE_NON_CONSERVEE:
            return []

        # arrondissements parisiens, marseillais et lyonnais
        if mairie["code"] in corr_plm:
            return corr_plm[mairie["code"]]

        if mairie["id"][-2:] == "01":
            return [("COM", mairie["code"])]

        nom_normalise = normaliser_nom(_nom_ville_from_nom_mairie(mairie["Nom"]))
        if (mairie["code"], nom_normalise) in corr_sous:
            return [corr_sous[(mairie["code"], nom_normalise)]]
        elif mairie["code"] in corr_direct:
            return [(corr_direct[mairie["code"]], mairie["code"])]

        sys.stderr.write(
            f"({mairie['id']!r}, {mairie['code']!r}, {mairie['Nom']!r}, {mairie['Adresse']['NomCommune']!r}),\n"
        )
        return []

    return commune_matcher


def post_traitement_mairies(source, corr_sous_communes, dest):
    mairies = []
    commune_matcher = obtenir_commune_matcher(corr_sous_communes)

    with open(source, "r") as f:
        for ligne in f:
            mairie = json.loads(ligne)

            adresse = (
                "\n".join(
                    [
                        *mairie["Adresse"]["Lignes"],
                        f"{mairie['Adresse']['CodePostal']} {mairie['Adresse']['NomCommune']}",
                    ]
                )
                if mairie["Adresse"]
                else ""
            )

            matches = commune_matcher(mairie)

            if "Localisation" in mairie["Adresse"]:
                localisation = Point(*mairie["Adresse"]["Localisation"][:2]).wkb_hex
            else:
                localisation = None

            for type, code in matches:
                mairies.append(
                    (
                        type,
                        code,
                        adresse,
                        mairie["Adresse"].get("Accessibilité", {}).get("type", "NAC"),
                        mairie["Adresse"].get("Accessibilité", {}).get("détail", ""),
                        localisation,
                        json.dumps(mairie.get("Ouvert", {}), separators=(",", ":")),
                        mairie.get("Contact", {}).get("Téléphone", ""),
                        mairie.get("Contact", {}).get("Email", ""),
                        mairie.get("Contact", {}).get("Url", ""),
                    )
                )

    with open(dest, "w") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "type",
                "code",
                "adresse",
                "accessibilite",
                "accessibilite_details",
                "localisation",
                "horaires",
                "telephone",
                "email",
                "site",
            ],
        )

        w.writerows(sorted(mairies, key=_commune_key))
