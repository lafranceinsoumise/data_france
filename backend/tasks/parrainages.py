import re
from enum import Enum

import pandas as pd
from doit.tools import create_folder

from sources import SOURCES, SOURCE_DIR, PREPARE_DIR
from tasks.cog import COG_DIR
from utils import normaliser_colonne


__all__ = ["task_traiter_parrainages"]


PARRAINAGES_DIR = PREPARE_DIR / "conseil_constitutionnel"
PARRAINAGES_MUNICIPAUX = PARRAINAGES_DIR / "parrainages_municipaux.csv"
PARRAINAGES_DEPARTEMENTAUX = PARRAINAGES_DIR / "parrainages_departementaux.csv"
PARRAINAGES_REGIONAUX = PARRAINAGES_DIR / "parrainages_regionaux.csv"


class NiveauMandat(Enum):
    MUNICIPAL = 1
    DEPARTEMENTAL = 2
    REGIONAL = 3


TYPES_MANDATS = {
    "Maire": NiveauMandat.MUNICIPAL,
    "Conseiller/ère départemental-e": NiveauMandat.DEPARTEMENTAL,
    "Maire délégué-e": NiveauMandat.MUNICIPAL,
    "Conseiller/ère régional-e": NiveauMandat.REGIONAL,
    "Membre du Conseil de Paris": NiveauMandat.MUNICIPAL,
    "Président-e d'un conseil de communauté de communes": NiveauMandat.MUNICIPAL,
    "Maire d'arrondissement": NiveauMandat.MUNICIPAL,
    "Conseiller/ère métropolitain-e de Lyon": NiveauMandat.DEPARTEMENTAL,
    "Président-e d'un conseil de métropole": NiveauMandat.MUNICIPAL,
    "Membre de l'assemblée de Corse": NiveauMandat.REGIONAL,
    "Président-e d'un conseil de communauté urbaine": NiveauMandat.MUNICIPAL,
    "Président-e d'un conseil de communauté d'agglomération": NiveauMandat.MUNICIPAL,
    "Membre de l'assemblée de Martinique": NiveauMandat.REGIONAL,
    "Membre de l'assemblée de Guyane": NiveauMandat.REGIONAL,
}


def task_traiter_parrainages():
    source = SOURCES.conseil_constitutionnel.parrainages2017

    parrainages = SOURCE_DIR / source.filename

    return {
        "file_dep": [
            parrainages,
            COG_DIR / "departements.csv",
            COG_DIR / "communes.csv",
        ],
        "targets": [
            PARRAINAGES_MUNICIPAUX,
            PARRAINAGES_DEPARTEMENTAUX,
            PARRAINAGES_REGIONAUX,
        ],
        "actions": [
            (create_folder, (PARRAINAGES_DIR,)),
            (
                traiter_parrainages,
                (
                    parrainages,
                    COG_DIR / "communes.csv",
                    COG_DIR / "departements.csv",
                    PARRAINAGES_MUNICIPAUX,
                    PARRAINAGES_DEPARTEMENTAUX,
                    PARRAINAGES_REGIONAUX,
                ),
            ),
        ],
    }


def traiter_parrainages(
    parrainages_path,
    communes_path,
    departements_path,
    dest_municipaux,
    dest_departementaux,
    dest_regionaux,
):
    parrainages = pd.read_csv(
        parrainages_path,
        skiprows=1,
        sep=";",
        names=[
            "civilite",
            "nom",
            "prenom",
            "mandat",
            "commune",
            "departement",
            "candidat",
            "date",
        ],
    )
    parrainages["sexe"] = parrainages.civilite.map({"M": "M", "Mme": "F"})

    communes = pd.read_csv(communes_path, dtype={"com": str})
    communes = communes[communes.typecom == "COM"]
    departements = pd.read_csv(departements_path, dtype={"dep": str, "reg": str})

    # régler une différence de ponctuation
    parrainages.loc[
        parrainages["departement"] == "Corse du Sud", "departement"
    ] = "Corse-du-Sud"

    mandats = parrainages.mandat.map(TYPES_MANDATS)
    parrainages["dep"] = parrainages["departement"].map(
        departements[["dep", "libelle"]].set_index("libelle")["dep"]
    )

    municipaux = parrainages.loc[mandats == NiveauMandat.MUNICIPAL, :].copy()
    departementaux = parrainages.loc[mandats == NiveauMandat.DEPARTEMENTAL, :]
    regionaux = parrainages.loc[mandats == NiveauMandat.REGIONAL, :]

    departementaux[["dep", "nom", "prenom", "sexe", "candidat"]].to_csv(
        dest_departementaux, index=False
    )

    regionaux.join(departements[["dep", "reg"]].set_index("dep"), on=["dep"])[
        ["reg", "dep", "nom", "prenom", "sexe", "candidat"]
    ].to_csv(dest_regionaux, index=False)

    municipaux["commune"] = (
        municipaux["commune"]
        .str.replace(
            r"^(.*) \((le|la|les)\)$", r"\2 \1", regex=True, flags=re.IGNORECASE
        )
        .str.replace(r"^(.*) \(l'\)$", r"L'\1", regex=True, flags=re.IGNORECASE)
    )
    municipaux.loc[municipaux["dep"] == "75", "commune"] = "Paris"
    municipaux["cle"] = normaliser_colonne(municipaux["commune"])

    communes["dep"] = communes["dep"].fillna(communes["com"].str.slice(0, 2))
    communes["cle"] = normaliser_colonne(communes["libelle"])

    municipaux.join(
        communes[["dep", "cle", "com"]].set_index(["dep", "cle"]), on=["dep", "cle"]
    ).reindex(columns=["com", "nom", "prenom", "sexe", "candidat"]).to_csv(
        dest_municipaux, index=False
    )
