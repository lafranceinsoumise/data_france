import re

import pandas as pd
from doit.tools import create_folder
from pandas.errors import OutOfBoundsDatetime

from sources import SOURCES, SOURCE_DIR, PREPARE_DIR
from utils import normaliser_colonne
from data_france.typologies import Fonction, ORDINAUX_LETTRES
from tasks.parrainages import PARRAINAGES_MUNICIPAUX

__all__ = ["task_traiter_elus_municipaux_epci"]

ORDINAL_RE = re.compile("^(\d+)(?:er|[eè]me)$")

CODES_FONCTION = {
    "maire délégué": Fonction.MAIRE_DELEGUE,
    "maire": Fonction.MAIRE,
    "vice-président": Fonction.VICE_PRESIDENT,
    "président": Fonction.PRESIDENT,
    "autre membre": Fonction.AUTRE_MEMBRE_COM,
    "adjoint au maire": Fonction.MAIRE_ADJOINT,
}

MUN_FIELDS = [
    "_code_dep",
    "_lib_dep",
    "_code_csp",
    "_nom_csp",
    "code",
    "_lib_commune",
    "nom",
    "prenom",
    "sexe",
    "date_naissance",
    "profession",
    "_lib_profession",
    "date_debut_mandat",
    "fonction",
    "date_debut_fonction",
    "nationalite",
]

EPCI_FIELDS = [
    "_code_dep",
    "_lib_dep",
    "_code_csp",
    "_lib_csp",
    "_siren",
    "_lib_epci",
    "code",
    "_lib_commune",
    "nom",
    "prenom",
    "sexe",
    "date_naissance",
    "_profession",
    "_lib_profession",
    "date_debut_mandat",
    "fonction",
    "date_debut_fonction",
]


corr_outremer = {
    "ZA": "97",
    "ZB": "97",
    "ZC": "97",
    "ZD": "97",
    "ZM": "97",
    "ZN": "NA",  # on ne gère pas ces outremers là pour le moment
    "ZP": "NA",
    "ZS": "NA",
    "ZW": "NA",
    "ZX": "NA",
}


def task_traiter_elus_municipaux_epci():
    RNE = SOURCES.interieur.rne
    rne_municipaux = SOURCE_DIR / RNE.municipaux.filename
    rne_epci = SOURCE_DIR / RNE.epci.filename
    dest = PREPARE_DIR / RNE.municipaux.filename

    return {
        "file_dep": [rne_municipaux, rne_epci, PARRAINAGES_MUNICIPAUX],
        "targets": [dest],
        "actions": [
            (create_folder, [dest.parent]),
            (
                traiter_elus_municipaux_ecpi,
                [rne_municipaux, rne_epci, PARRAINAGES_MUNICIPAUX, dest],
            ),
        ],
    }


def normaliser_fonction(s):
    if pd.isna(s):
        return s

    words = s.lower().split()

    if words[0].lower() in ORDINAUX_LETTRES:
        ordre = ORDINAUX_LETTRES.index(words[0]) + 1
        words.pop(0)
    elif m := ORDINAL_RE.match(words[0]):
        ordre = int(m.group(1))
        words.pop(0)
    else:
        ordre = pd.NA

    while words and " ".join(words) not in CODES_FONCTION:
        words.pop()

    return CODES_FONCTION[" ".join(words)] if words else pd.NA, ordre


def parser_dates(df):
    for c in df.columns:
        if c.startswith("date_"):
            try:
                date_corrigee = (
                    df[c]
                    .str.replace(r"/00(\d{2})$", r"/20\1", regex=True)
                    .str.replace(r"/0990$", r"/1990", regex=True)
                )
                df[c] = pd.to_datetime(date_corrigee, format="%d/%m/%Y")
            except OutOfBoundsDatetime:
                raise ValueError(f"Colonne {c}")


def traiter_elus_municipaux_ecpi(municipaux_path, epci_path, parrainages_path, dest):
    mun = pd.read_csv(
        municipaux_path,
        sep="\t",
        encoding="utf8",
        skiprows=2,
        names=MUN_FIELDS,
        na_values=[""],
        keep_default_na=False,
        usecols=[f for f in MUN_FIELDS if not f.startswith("_")],
        dtype={"code": str, "profession": str},
    )

    mun = mun.drop_duplicates(["code", "nom", "prenom", "date_naissance"])

    parser_dates(mun)
    fonctions = mun["fonction"].map(normaliser_fonction)
    mun["fonction"] = fonctions.str.get(0)
    mun["ordre_fonction"] = fonctions.str.get(1)

    ep = pd.read_csv(
        epci_path,
        sep="\t",
        encoding="utf8",
        skiprows=2,
        names=EPCI_FIELDS,
        na_values=[""],
        keep_default_na=False,
        usecols=[f for f in EPCI_FIELDS if not f.startswith("_")],
        dtype={"code": str, "profession": str},
    )

    parser_dates(ep)
    fonctions = ep["fonction"].map(normaliser_fonction)
    ep["fonction"] = fonctions.str.get(0)
    ep = ep.sort_values(
        [
            "code",
            "nom",
            "prenom",
            "sexe",
            "date_naissance",
            "date_debut_mandat",
            "date_debut_fonction",
        ]
    ).drop_duplicates(["code", "nom", "prenom", "sexe", "date_naissance"], keep="last")

    res = pd.merge(
        mun,
        ep,
        how="left",
        on=["code", "nom", "prenom", "sexe", "date_naissance"],
        suffixes=["", "_epci"],
    )

    parrainages = pd.read_csv(parrainages_path, dtype={"com": str})

    for df in (res, parrainages):
        df["cle_nom"] = normaliser_colonne(df["nom"])
        df["cle_prenom"] = normaliser_colonne(df["prenom"])

    parrainages = parrainages.rename(columns={"candidat": "parrainage2017"})

    res = res.join(
        parrainages.set_index(["com", "cle_nom", "cle_prenom"])["parrainage2017"],
        on=["code", "cle_nom", "cle_prenom"],
    )
    del res["cle_nom"]
    del res["cle_prenom"]

    res.to_csv(dest, index=False)
