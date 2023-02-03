import contextlib
import csv
import json
import lzma
import re
from datetime import datetime
from operator import itemgetter
from pathlib import Path

import pandas as pd
from glom import glom, T, Invoke, Match, Switch, Regex, Not, Coalesce, Iter, Val
from shapely.geometry import MultiPolygon, shape

from data_france.utils import TypeNom
from sources import BASE_DIR, SOURCE_DIR, PREPARE_DIR, SOURCES
from tasks.admin_express import COMMUNES_GEOMETRY, CANTONS_GEOMETRY
from tasks.annuaire_administratif import MAIRIES_TRAITEES
from tasks.assemblee_nationale import ASSEMBLEE_NATIONALE_DIR
from tasks.cog import (
    DEPARTEMENTS_COG,
    REGIONS_COG,
    COLLECTIVITES_DEPARTEMENTALES_COG,
    COMMUNES_CSV,
    EPCI_CSV,
    CANTONS_CSV,
    COMMUNE_TYPE_ORDERING,
)

CODES_POSTAUX = SOURCE_DIR / "laposte" / "codes_postaux.csv"

REFERENCES_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_france" / "data"

CTU = DATA_DIR / "ctu.csv"

FINAL_REGIONS = DATA_DIR / "regions.csv.lzma"
FINAL_DEPARTEMENTS = DATA_DIR / "departements.csv.lzma"
FINAL_EPCI = DATA_DIR / "epci.csv.lzma"
FINAL_COMMUNES = DATA_DIR / "communes.csv.lzma"
FINAL_CODES_POSTAUX = DATA_DIR / "codes_postaux.csv.lzma"
FINAL_CORRESPONDANCES_CODE_POSTAUX = DATA_DIR / "codes_postaux_communes.csv.lzma"
FINAL_CANTONS = DATA_DIR / "cantons.csv.lzma"
FINAL_CIRCONSCRIPTIONS_CONSULAIRES = DATA_DIR / "circonscriptions_consulaires.csv.lzma"
FINAL_CIRCONSCRIPTIONS_LEGISLATIVES = (
    DATA_DIR / "circonscriptions_legislatives.csv.lzma"
)
FINAL_COLLECTIVITES_DEPARTEMENTALES = (
    DATA_DIR / "collectivites_departementales.csv.lzma"
)
FINAL_COLLECTIVITES_REGIONALES = DATA_DIR / "collectivites_regionales.csv.lzma"
FINAL_DEPUTES = DATA_DIR / "deputes.csv.lzma"
FINAL_DEPUTES_EUROPEENS = DATA_DIR / "deputes_europeens.csv.lzma"
FINAL_ELUS_MUNICIPAUX = DATA_DIR / "elus_municipaux.csv.lzma"
FINAL_ELUS_DEPARTEMENTAUX = DATA_DIR / "elus_departementaux.csv.lzma"
FINAL_ELUS_REGIONAUX = DATA_DIR / "elus_regionaux.csv.lzma"

NULL = r"\N"

INTERIEUR_VERS_DEPARTEMENT = {
    "ZA": "971",
    "ZB": "972",
    "ZC": "973",
    "ZD": "974",
    "ZM": "976",
    "ZN": "988",
    "ZP": "987",
    "ZS": "975",
    "ZW": "986",
    "ZX": "977",  # choix fait par l'AN pour la circo concernée
}

NON_DEPARTEMENT = re.compile(r"^9(?:7[57]|8\d)$")


def code_circonscription(prop):
    dep = INTERIEUR_VERS_DEPARTEMENT.get(prop["code_dpt"], prop["code_dpt"])
    num = prop["num_circ"].rjust(2, "0")
    return f"{dep}-{num}"


__all__ = [
    "task_generer_fichier_regions",
    "task_generer_fichier_collectivites_regionales",
    "task_generer_fichier_departements",
    "task_generer_fichier_collectivites_departementales",
    "task_generer_fichier_epci",
    "task_generer_fichier_communes",
    "task_generer_fichier_codes_postaux",
    "task_generer_fichier_cantons",
    "task_generer_fichier_circonscriptions_consulaires",
    "task_generer_fichier_circonscriptions_legislatives",
    "task_generer_fichier_elus_municipaux",
    "task_generer_fichier_elus_departementaux",
    "task_generer_fichier_elus_regionaux",
    "task_generer_fichier_deputes",
    "task_generer_fichier_deputes_europeens",
]


def commune_key(t):
    return (COMMUNE_TYPE_ORDERING.index(t["type"]), t["code"])


@contextlib.contextmanager
def id_from_file(path, read_only=False):
    reference = {}

    full_path = REFERENCES_DIR / path

    with open(full_path, "r") as f:
        r = csv.DictReader(f)
        columns = [c for c in r.fieldnames if c != "id"]

        for d in r:
            id = d.pop("id")
            reference[tuple(d[c] for c in columns)] = int(id)

    last_id = max(reference.values()) if reference else -1

    def get_id(**kwargs):
        nonlocal last_id
        key = tuple(str(kwargs.pop(c)) for c in columns)

        if kwargs:
            raise ValueError(f"Colonnes inconnues ({', '.join(kwargs)}) dans {path}")

        if key in reference:
            return reference[key]
        else:
            if read_only:
                raise ValueError(f"ID inconnue pour {kwargs!r} dans {path}")
            last_id += 1
            return reference.setdefault(key, last_id)

    yield get_id

    with open(full_path, "w") as f:
        w = csv.writer(f)
        w.writerow(columns + ["id"])
        w.writerows([*t, id] for t, id in reference.items())


def task_generer_fichier_regions():
    src = REGIONS_COG
    return {
        "file_dep": [src],
        "targets": [FINAL_REGIONS],
        "actions": [(generer_fichier_regions, [src, FINAL_REGIONS])],
    }


def task_generer_fichier_collectivites_regionales():
    return {
        "file_dep": [REGIONS_COG, CTU],
        "targets": [FINAL_COLLECTIVITES_REGIONALES],
        "actions": [
            (
                generer_fichier_collectivites_regionales,
                [REGIONS_COG, CTU, FINAL_COLLECTIVITES_REGIONALES],
            )
        ],
    }


def task_generer_fichier_departements():
    src = DEPARTEMENTS_COG
    return {
        "file_dep": [src],
        "targets": [FINAL_DEPARTEMENTS],
        "actions": [(generer_fichier_departements, [src, FINAL_DEPARTEMENTS])],
    }


def task_generer_fichier_collectivites_departementales():
    src = COLLECTIVITES_DEPARTEMENTALES_COG
    return {
        "file_dep": [src],
        "targets": [FINAL_COLLECTIVITES_DEPARTEMENTALES],
        "actions": [
            (
                generer_fichier_collectivites_departementales,
                [src, FINAL_COLLECTIVITES_DEPARTEMENTALES],
            )
        ],
    }


def task_generer_fichier_epci():
    return {
        "file_dep": [EPCI_CSV],
        "targets": [FINAL_EPCI],
        "actions": [(generer_fichier_epci, [EPCI_CSV, FINAL_EPCI])],
    }


def task_generer_fichier_communes():
    return {
        "file_dep": [COMMUNES_CSV, COMMUNES_GEOMETRY, MAIRIES_TRAITEES],
        "targets": [FINAL_COMMUNES],
        "actions": [
            (
                generer_fichier_communes,
                [COMMUNES_CSV, COMMUNES_GEOMETRY, MAIRIES_TRAITEES, FINAL_COMMUNES],
            ),
        ],
    }


def task_generer_fichier_codes_postaux():
    return {
        "file_dep": [CODES_POSTAUX, COMMUNES_CSV],
        "targets": [FINAL_CODES_POSTAUX, FINAL_CORRESPONDANCES_CODE_POSTAUX],
        "actions": [
            (
                generer_fichiers_codes_postaux,
                [
                    CODES_POSTAUX,
                    COMMUNES_CSV,
                    FINAL_CODES_POSTAUX,
                    FINAL_CORRESPONDANCES_CODE_POSTAUX,
                ],
            )
        ],
    }


def task_generer_fichier_cantons():
    return {
        "file_dep": [CANTONS_CSV, REFERENCES_DIR / "communes.csv", CANTONS_GEOMETRY],
        "targets": [FINAL_CANTONS],
        "actions": [
            (
                generer_fichier_cantons,
                [CANTONS_CSV, CANTONS_GEOMETRY, FINAL_CANTONS],
            )
        ],
    }


def task_generer_fichier_circonscriptions_consulaires():
    return {
        "file_dep": [REFERENCES_DIR / "circonscriptions_consulaires.csv"],
        "targets": [FINAL_CIRCONSCRIPTIONS_CONSULAIRES],
        "actions": [
            (
                generer_fichier_circonscriptions_consulaires,
                (
                    REFERENCES_DIR / "circonscriptions_consulaires.csv",
                    FINAL_CIRCONSCRIPTIONS_CONSULAIRES,
                ),
            )
        ],
    }


def task_generer_fichier_circonscriptions_legislatives():
    source = (
        SOURCE_DIR / SOURCES.sciences_po.contours_circonscriptions_legislatives.filename
    )
    return {
        "file_dep": [source],
        "targets": [FINAL_CIRCONSCRIPTIONS_LEGISLATIVES],
        "actions": [
            (
                generer_fichier_circonscriptions_legislatives,
                (source, FINAL_CIRCONSCRIPTIONS_LEGISLATIVES),
            )
        ],
    }


def task_generer_fichier_elus_municipaux():
    source_file = PREPARE_DIR / SOURCES.interieur.rne.municipaux.filename
    return {
        "file_dep": [source_file, COMMUNES_CSV],
        "task_dep": ["generer_fichier_communes"],
        "targets": [FINAL_ELUS_MUNICIPAUX],
        "actions": [
            (
                generer_fichier_elus_municipaux,
                (source_file, COMMUNES_CSV, FINAL_ELUS_MUNICIPAUX),
            )
        ],
    }


def task_generer_fichier_elus_departementaux():
    source_file = PREPARE_DIR / SOURCES.interieur.rne.departementaux.filename
    return {
        "file_dep": [source_file],
        "targets": [FINAL_ELUS_DEPARTEMENTAUX],
        "actions": [
            (
                generer_fichier_elus_departementaux,
                (source_file, FINAL_ELUS_DEPARTEMENTAUX),
            )
        ],
    }


def task_generer_fichier_elus_regionaux():
    source_file = PREPARE_DIR / SOURCES.interieur.rne.regionaux.filename
    return {
        "file_dep": [source_file, CTU],
        "targets": [FINAL_ELUS_REGIONAUX],
        "actions": [
            (generer_fichier_elus_regionaux, (source_file, CTU, FINAL_ELUS_REGIONAUX))
        ],
    }


def task_generer_fichier_deputes():
    sources = {
        f"{c}_path": ASSEMBLEE_NATIONALE_DIR / f"{c}.csv"
        for c in ["deputes", "groupes", "partis", "deputes_groupes", "deputes_partis"]
    }

    return {
        "file_dep": list(sources.values()),
        "task_dep": ["generer_fichier_circonscriptions_legislatives"],
        "targets": [FINAL_DEPUTES],
        "actions": [
            (
                generer_fichier_deputes,
                (),
                {**sources, "dest": FINAL_DEPUTES},
            )
        ],
    }


def task_generer_fichier_deputes_europeens():
    source = PREPARE_DIR / SOURCES.interieur.rne.europeens.filename

    return {
        "file_dep": [source],
        "targets": [FINAL_DEPUTES_EUROPEENS],
        "actions": [
            (generer_fichiers_deputes_europeens, (source, FINAL_DEPUTES_EUROPEENS))
        ],
    }


def generer_fichier_regions(path, lzma_path):
    with open(path, "r") as f, lzma.open(lzma_path, "wt") as l, id_from_file(
        "regions.csv"
    ) as region_id, id_from_file("communes.csv", True) as commune_id:
        r = csv.DictReader(f)
        w = csv.DictWriter(
            l, fieldnames=["id", "code", "nom", "type_nom", "chef_lieu_id"]
        )
        w.writeheader()
        w.writerows(
            {
                "id": region_id(code=region["REG"]),
                "code": region["REG"],
                "nom": region["NCCENR"],
                "type_nom": region["TNCC"],
                "chef_lieu_id": commune_id(type="COM", code=region["CHEFLIEU"]),
            }
            for region in r
        )


def generer_fichier_departements(path, lzma_path):
    with open(path, "r") as f, lzma.open(lzma_path, "wt") as l, id_from_file(
        "departements.csv"
    ) as departement_id, id_from_file("regions.csv", True) as region_id, id_from_file(
        "communes.csv", True
    ) as commune_id:
        r = csv.DictReader(f)
        w = csv.DictWriter(
            l, fieldnames=["id", "code", "nom", "type_nom", "chef_lieu_id", "region_id"]
        )
        w.writeheader()
        w.writerows(
            {
                "id": departement_id(code=d["DEP"]),
                "code": d["DEP"],
                "nom": d["NCCENR"],
                "type_nom": d["TNCC"],
                "chef_lieu_id": commune_id(type="COM", code=d["CHEFLIEU"]),
                "region_id": region_id(code=d["REG"]),
            }
            for d in r
        )


def generer_fichier_collectivites_departementales(col_dep_path, lzma_path):
    with open(col_dep_path) as f, lzma.open(lzma_path, "wt") as l, id_from_file(
        "collectivites_departementales.csv"
    ) as id_coldep, id_from_file("regions.csv", read_only=True) as id_region:
        r = csv.DictReader(f)
        w = csv.DictWriter(
            l, fieldnames=["id", "code", "type", "nom", "type_nom", "region_id"]
        )

        w.writeheader()
        w.writerows(
            [
                {
                    "id": id_coldep(code=c["CTCD"]),
                    "code": c["CTCD"],
                    "type": "D" if c["CTCD"].endswith("D") else "S",
                    "type_nom": c["TNCC"],
                    "nom": c["NCCENR"],
                    "region_id": id_region(code=c["REG"]),
                }
                for c in r
            ]
        )


def generer_fichier_collectivites_regionales(reg_path, ctu_path, lzma_path):
    regions = pd.read_csv(reg_path, dtype={"REG": str})
    ctu = pd.read_csv(ctu_path, dtype={"code_region": str}).set_index("code_region")
    with id_from_file("regions.csv") as reg_id:
        regions["id"] = regions.REG.map(lambda r: reg_id(code=r))

    est_ctu = regions.REG.isin(ctu.index)

    codes = regions.REG.map(ctu.code).where(est_ctu, regions.REG + "R")
    type_col = est_ctu.map({True: "U", False: "R"})
    charnieres = regions.TNCC.map(lambda t: TypeNom(t).charniere)
    noms = regions.REG.map(ctu.nom).where(
        regions.REG.isin(est_ctu), "Conseil régional " + charnieres + regions.LIBELLE
    )

    type_nom = regions.REG.map(ctu.type_nom).where(est_ctu, 2).convert_dtypes()
    region_id = regions.id

    with id_from_file("collectivites_regionales.csv") as colreg_id:
        id = codes.map(lambda c: colreg_id(code=c))

    colreg = pd.DataFrame(
        {
            "id": id,
            "code": codes,
            "nom": noms,
            "region_id": region_id,
            "type_nom": type_nom,
            "type": type_col,
        }
    )

    with lzma.open(lzma_path, "wb") as l:
        colreg.to_csv(l, index=False)


def generer_fichier_epci(path, lzma_path):
    with open(path, "r") as f, lzma.open(lzma_path, "wt") as l, id_from_file(
        "epci.csv"
    ) as get_id:

        r = csv.DictReader(f)
        w = csv.DictWriter(l, fieldnames=["id", *r.fieldnames])
        w.writeheader()
        w.writerows({"id": get_id(code=epci["code"]), **epci} for epci in r)


COMMUNES_FIELDS = [
    "id",
    "code",
    "type",
    "nom",
    "type_nom",
    "population_municipale",
    "population_cap",
    "departement_id",
    "commune_parent_id",
    "epci_id",
    "geometry",
    "mairie_adresse",
    "mairie_accessibilite",
    "mairie_accessibilite_details",
    "mairie_localisation",
    "mairie_horaires",
    "mairie_email",
    "mairie_telephone",
    "mairie_site",
]


def _joiner_generator(r: csv.DictReader, key):
    current_entry = next(r)
    current_key = key(current_entry)
    k = yield

    try:
        while True:
            while current_key < k:
                current_entry = next(r)
                current_key = key(current_entry)
            if k == current_key:
                k = yield current_entry
            else:
                k = yield {}
    except StopIteration:
        while True:
            yield {}


def generer_fichier_communes(communes, communes_geo, mairies, dest):
    csv.field_size_limit(2 * 131072)  # double default limit

    with open(communes, "r", newline="") as fc, open(
        communes_geo, "r", newline=""
    ) as fg, open(mairies, "r", newline="") as fm, lzma.open(
        dest, "wt"
    ) as fl, id_from_file(
        "communes.csv"
    ) as commune_id, id_from_file(
        "epci.csv", True
    ) as epci_id, id_from_file(
        "departements.csv", True
    ) as departement_id:
        rc = csv.DictReader(fc)
        rg = csv.DictReader(fg)
        rm = csv.DictReader(fm)

        geometry_cr = _joiner_generator(rg, commune_key)
        mairie_cr = _joiner_generator(rm, commune_key)
        next(geometry_cr)
        next(mairie_cr)

        w = csv.DictWriter(fl, fieldnames=COMMUNES_FIELDS)
        w.writeheader()

        for commune in rc:
            k = commune_key(commune)
            geometry = geometry_cr.send(k)
            mairie = mairie_cr.send(k)

            w.writerow(
                {
                    "id": commune_id(type=commune["type"], code=commune["code"]),
                    "code": commune["code"],
                    "type": commune["type"],
                    "nom": commune["nom"],
                    "type_nom": commune["type_nom"],
                    "population_municipale": commune["population_municipale"] or NULL,
                    "population_cap": commune["population_cap"] or NULL,
                    "departement_id": departement_id(code=commune["code_departement"])
                    if commune["code_departement"]
                    else NULL,
                    "commune_parent_id": commune_id(
                        type="COM", code=commune["commune_parent"]
                    )
                    if commune["commune_parent"]
                    else NULL,
                    "epci_id": epci_id(code=commune["epci"])
                    if commune["epci"]
                    else NULL,
                    "geometry": geometry.get("geometry", NULL),
                    "mairie_adresse": mairie.get("adresse"),
                    "mairie_accessibilite": mairie.get("accessibilite"),
                    "mairie_accessibilite_details": mairie.get("accessibilite_details"),
                    "mairie_localisation": mairie.get("localisation") or NULL,
                    "mairie_horaires": mairie.get("horaires", "[]"),
                    "mairie_email": mairie.get("email"),
                    "mairie_telephone": mairie.get("telephone"),
                    "mairie_site": mairie.get("site"),
                }
            )


def generer_fichiers_codes_postaux(
    codes_postaux, communes, final_code_postal, final_corr
):
    communes = pd.read_csv(communes, usecols=["type", "code"], dtype={"code": str})
    communes["type"] = pd.Categorical(
        communes["type"], categories=["COM", "ARM", "COMA", "COMD"]
    )
    communes = (
        communes.sort_values(["type", "code"])
        .drop_duplicates(["code"])
        .set_index(["code"])["type"]
    )

    codes_postaux = pd.read_csv(
        codes_postaux,
        dtype={"Code_commune_INSEE": str, "Code_postal": str},
        usecols=["Code_commune_INSEE", "Code_postal"],
    ).drop_duplicates()

    # La commune Les Trois Lacs a changé de code INSEE au 01/01/2021 et ce n'est
    # pas pris en compte par la poste, donc modification manuelle
    codes_postaux.loc[
        codes_postaux.Code_commune_INSEE == "27676", "Code_commune_INSEE"
    ] = "27058"

    with id_from_file("codes_postaux.csv") as id_code_postal, id_from_file(
        "communes.csv"
    ) as id_commune:
        with lzma.open(final_code_postal, "wt", newline="") as fl:
            w = csv.DictWriter(fl, fieldnames=["id", "code"])
            w.writeheader()

            w.writerows(
                {"id": id_code_postal(code=code), "code": code}
                for code in codes_postaux["Code_postal"].unique()
            )

        with lzma.open(final_corr, "wt", newline="") as fl:
            w = csv.DictWriter(fl, fieldnames=["codepostal_id", "commune_id"])
            w.writeheader()

            w.writerows(
                {
                    "codepostal_id": id_code_postal(code=ligne.Code_postal),
                    "commune_id": id_commune(
                        type=communes.loc[ligne.Code_commune_INSEE],
                        code=ligne.Code_commune_INSEE,
                    ),
                }
                for ligne in codes_postaux.itertuples()
                if ligne.Code_commune_INSEE in communes.index
            )


def generer_fichier_cantons(
    cantons,
    geometries,
    final_cantons,
):
    csv.field_size_limit(5 * 131072)  # quintuple default limit
    with id_from_file("cantons.csv") as canton_id, id_from_file(
        "communes.csv"
    ) as commune_id, id_from_file("departements.csv") as departement_id, open(
        cantons,
    ) as f_cantons, open(
        geometries
    ) as f_geometries, lzma.open(
        final_cantons, "wt", newline=""
    ) as fl:
        r = csv.DictReader(f_cantons)
        gr = csv.DictReader(f_geometries)
        gj = _joiner_generator(gr, itemgetter("code"))
        next(gj)

        w = csv.DictWriter(
            fl,
            fieldnames=[
                "id",
                "code",
                "type",
                "composition",
                "nom",
                "type_nom",
                "departement_id",
                "bureau_centralisateur_id",
                "geometry",
            ],
            extrasaction="ignore",
        )
        w.writeheader()

        w.writerows(
            {
                **canton,
                "id": canton_id(code=canton["code"]),
                "bureau_centralisateur_id": commune_id(
                    type="COM", code=canton["bureau_centralisateur"]
                )
                if canton["bureau_centralisateur"]
                else r"\N",
                "departement_id": departement_id(code=canton["departement"]),
                "composition": canton["composition"] or r"\N",
                "geometry": gj.send(canton["code"]).get("geometry", NULL),
            }
            for canton in r
        )


def generer_fichier_circonscriptions_consulaires(source, dest):
    """Le fichier source a été généré à partir de l'arrêté ministériel"""
    with open(source, "r") as f_in, lzma.open(dest, "wt") as f_out:
        reader = csv.DictReader(f_in, delimiter=";")
        writer = csv.DictWriter(
            f_out,
            fieldnames=["id", "nom", "consulats", "nombre_conseillers", "pays"],
            extrasaction="ignore",
        )
        writer.writeheader()

        with id_from_file("circonscriptions_legislatives.csv") as id_circo_leg:
            for circ in reader:
                cons = ", ".join(f'"{c}"' for c in circ["consulats"].split("/"))
                circ["consulats"] = f"{{{cons}}}"
                circ["circonscription_legislative_id"] = id_circo_leg(
                    code=circ["circonscription_législative"]
                )

                writer.writerow(circ)


def geometrie_circonscription(geom):
    s = shape(geom)

    if not isinstance(s, MultiPolygon):
        s = MultiPolygon([s])
    return s.wkb_hex


def generer_fichier_circonscriptions_legislatives(source, dest):
    """À partir du fichier des circonscriptions parlementaires de Sciences Po"""

    with source.open() as f:
        circos = json.load(f)

    with id_from_file("departements.csv") as id_dep, id_from_file(
        "circonscriptions_legislatives.csv"
    ) as id_circ:
        spec = {
            "id": ("properties", code_circonscription, Invoke(id_circ).specs(code=T)),
            "code": ("properties", code_circonscription),
            "departement_id": (
                "properties.code_dpt",
                Invoke(INTERIEUR_VERS_DEPARTEMENT.get).specs(T, T),
                Match(
                    Switch({Not(Regex(NON_DEPARTEMENT)): Invoke(id_dep).specs(code=T)}),
                    default=NULL,
                ),
            ),
            "geometry": ("geometry", geometrie_circonscription),
        }

        with lzma.open(dest, "wt") as f:
            w = csv.DictWriter(f, fieldnames=spec)
            w.writeheader()
            w.writerows(
                sorted(glom(circos["features"], [spec]), key=itemgetter("code"))
            )

            for i in range(1, 12):
                code = f"99-{i:02d}"
                w.writerow(
                    {
                        "id": id_circ(code=code),
                        "code": code,
                        "departement_id": NULL,
                        "geometry": NULL,
                    }
                )


def normaliser_date(d):
    """Normalise une date au format ISO"""
    d = datetime.strptime(d, "%d/%m/%Y")
    if d.year < 100:
        d = d.replace(year=2000 + d.year)
    return d.strftime("%Y-%m-%d")


def generer_fichier_elus_municipaux(elus_municipaux, communes, final_elus):
    coms = pd.read_csv(communes, usecols=["type", "code"], dtype={"code": str})
    coms = set(coms[coms.type == "COM"]["code"])

    with id_from_file("communes.csv", read_only=True) as id_commune, id_from_file(
        "elus_municipaux.csv"
    ) as id_elu, open(elus_municipaux, newline="") as f, lzma.open(
        final_elus, "wt", newline=""
    ) as fl:
        r = csv.DictReader(f)

        w = csv.DictWriter(
            fl,
            fieldnames=[
                "id",
                "commune_id",
                "nom",
                "prenom",
                "sexe",
                "date_naissance",
                "profession",
                "date_debut_mandat",
                "fonction",
                "ordre_fonction",
                "date_debut_fonction",
                "date_debut_mandat_epci",
                "fonction_epci",
                "date_debut_fonction_epci",
                "nationalite",
                "parrainage2017",
            ],
        )
        w.writeheader()

        for l in r:
            code = l.pop("code")

            # prendre en compte le changement de code INSEE des Trois Lacs
            if code == "27676":
                code = "27058"

            if code not in coms:
                continue
            l["commune_id"] = id_commune(code=code, type="COM")

            for f in [
                "date_debut_fonction",
                "date_debut_mandat_epci",
                "date_debut_fonction_epci",
                "ordre_fonction",
                "profession",
            ]:
                if not l[f]:
                    l[f] = "\\N"

            # attention: utiliser la date de naissance normalisée et l'id commune
            l["id"] = id_elu(
                commune_id=str(
                    l["commune_id"]
                ),  # attention l'id est interprétée comme str
                nom=l["nom"],
                prenom=l["prenom"],
                sexe=l["sexe"],
                date_naissance=l["date_naissance"],
            )

            w.writerow({k: v for k, v in l.items() if not k[0] == "_"})


def generer_fichier_elus_departementaux(source, dest):
    with id_from_file("cantons.csv") as id_canton, id_from_file(
        "elus_departementaux.csv"
    ) as id_elu, source.open("r") as i, lzma.open(dest, "wt") as d:
        r = csv.DictReader(i)
        w = csv.DictWriter(
            d,
            fieldnames=[
                "id",
                "canton_id",
                "nom",
                "prenom",
                "sexe",
                "date_naissance",
                "profession",
                "date_debut_mandat",
                "fonction",
                "ordre_fonction",
                "date_debut_fonction",
            ],
        )
        w.writeheader()

        for elu in r:
            canton_id = id_canton(code=elu.pop("code"))

            for f in [
                "date_debut_fonction",
                "ordre_fonction",
                "profession",
            ]:
                if not elu[f]:
                    elu[f] = "\\N"

            w.writerow(
                {
                    **elu,
                    "id": id_elu(
                        canton_id=canton_id,
                        nom=elu["nom"],
                        prenom=elu["prenom"],
                        sexe=elu["sexe"],
                        date_naissance=elu["date_naissance"],
                    ),
                    "canton_id": canton_id,
                }
            )


def generer_fichier_elus_regionaux(source, ctu_path, dest):
    ctu = dict(
        pd.read_csv(ctu_path, dtype={"code_region": str}).set_index("code_region")[
            "code"
        ]
    )

    with id_from_file("collectivites_regionales.csv") as id_colreg, id_from_file(
        "collectivites_departementales.csv"
    ) as id_coldep, id_from_file("elus_regionaux.csv") as id_elu, source.open(
        "r"
    ) as i, lzma.open(
        dest, "wt"
    ) as d:
        r = csv.DictReader(i)
        w = csv.DictWriter(
            d,
            fieldnames=[
                "id",
                "collectivite_regionale_id",
                "collectivite_departementale_id",
                "nom",
                "prenom",
                "sexe",
                "date_naissance",
                "profession",
                "date_debut_mandat",
                "fonction",
                "ordre_fonction",
                "date_debut_fonction",
            ],
        )
        w.writeheader()

        for elu in r:
            code_region = elu.pop("code")
            code_colreg = ctu.get(code_region, f"{code_region}R")
            colreg_id = id_colreg(code=code_colreg)

            code_dep = elu.pop("code_sec")
            if code_dep == "75":
                code_dep = "75C"
            elif code_dep[-1] not in ("E", "M"):
                code_dep = f"{code_dep}D"

            for f in [
                "date_debut_fonction",
                "ordre_fonction",
                "profession",
            ]:
                if not elu[f]:
                    elu[f] = NULL

            w.writerow(
                {
                    **elu,
                    "id": id_elu(
                        collectivite_regionale_id=colreg_id,
                        nom=elu["nom"],
                        prenom=elu["prenom"],
                        sexe=elu["sexe"],
                        date_naissance=elu["date_naissance"],
                    ),
                    "collectivite_regionale_id": colreg_id,
                    "collectivite_departementale_id": id_coldep(code=code_dep),
                }
            )


def generer_fichier_deputes(
    deputes_path,
    groupes_path,
    partis_path,
    deputes_groupes_path,
    deputes_partis_path,
    dest,
):
    deputes = pd.read_csv(deputes_path)
    groupes = pd.read_csv(groupes_path)
    deputes_groupes = pd.read_csv(deputes_groupes_path).join(
        groupes.set_index("code")[["nom", "sigle"]], on="code"
    )
    deputes_groupes = (
        deputes_groupes[deputes_groupes.date_fin.isnull()]
        .sort_values(["code_depute", "relation"])
        .drop_duplicates(
            "code_depute", keep="last"
        )  # garder "P" (président) plutôt que "M" (membre)
        .set_index("code_depute")
    )
    deputes_groupes["groupe"] = deputes_groupes.nom + " (" + deputes_groupes.sigle + ")"

    partis = pd.read_csv(partis_path)
    deputes_partis = pd.read_csv(deputes_partis_path).join(
        partis.set_index("code")[["nom", "sigle"]], on="code"
    )
    deputes_partis = deputes_partis[deputes_partis.date_fin.isnull()].set_index(
        "code_depute"
    )
    deputes_partis = deputes_partis.nom + " (" + deputes_partis.sigle + ")"
    deputes_partis.name = "parti"

    deputes = deputes.join(deputes_groupes[["groupe", "relation"]], on=["code"]).join(
        deputes_partis, on=["code"]
    )

    with lzma.open(dest, "wt") as f, id_from_file(
        "circonscriptions_legislatives.csv"
    ) as id_circos, id_from_file("deputes.csv") as id_deputes:

        spec = {
            "id": Invoke(id_deputes).specs(code=T.code),
            "circonscription_id": Invoke(id_circos).specs(code=T.circonscription),
            **{
                c: getattr(T, c)
                for c in [
                    "code",
                    "nom",
                    "prenom",
                    "sexe",
                    "date_naissance",
                    "legislature",
                    "date_debut_mandat",
                ]
            },
            "groupe": Coalesce(T.groupe, skip=pd.isna, default=""),
            "parti": Coalesce(T.parti, skip=pd.isna, default=""),
            "date_fin_mandat": Coalesce(T.date_fin_mandat, skip=pd.isna, default=NULL),
            "relation": Coalesce(T.relation, skip=pd.isna, default=""),
            "profession": Val(NULL),
        }

        w = csv.DictWriter(f, fieldnames=spec)
        w.writeheader()
        w.writerows(glom(deputes.itertuples(), Iter(spec)))


def generer_fichiers_deputes_europeens(source, dest):
    with open(source) as s_fd, lzma.open(dest, "wt") as d_fd, id_from_file(
        "deputes_europeens.csv"
    ) as id:
        r = csv.DictReader(s_fd)
        w = csv.DictWriter(
            d_fd,
            fieldnames=[
                "id",
                "nom",
                "prenom",
                "sexe",
                "date_naissance",
                "profession",
                "date_debut_mandat",
            ],
        )

        w.writeheader()

        for e in r:
            if not e["profession"]:
                e["profession"] = NULL

            w.writerow(
                {
                    "id": id(
                        nom=e["nom"],
                        prenom=e["prenom"],
                        date_naissance=e["date_naissance"],
                        sexe=e["sexe"],
                    ),
                    **e,
                }
            )
