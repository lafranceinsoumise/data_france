import lzma
from pathlib import Path

import pandas as pd

__all__ = ["task_traiter_epci", "task_traiter_communes"]

from doit.tools import create_folder

BASE_PATH = Path(__file__).parent.parent.parent
PREPARE_DIR = BASE_PATH / "build" / "prepare"
DEST_DIR = BASE_PATH / "data_france" / "data"

EPCI_XLS = PREPARE_DIR / "insee" / "intercommunalite" / "epci.xls"
COMMUNES_CSV = PREPARE_DIR / "insee" / "cog" / "communes.csv"

COMMUNES_POPULATION = PREPARE_DIR / "insee" / "population" / "Communes.csv"
COMMUNES_AD_POPULATION = (
    PREPARE_DIR / "insee" / "population" / "Communes_associees_ou_deleguees.csv"
)


def task_traiter_epci():
    target = DEST_DIR / "epci.csv.lzma"

    return {
        "file_dep": [EPCI_XLS],
        "targets": [target],
        "actions": [
            (create_folder, [target.parent]),
            (traiter_epci, [EPCI_XLS, target]),
        ],
    }


def task_traiter_communes():
    target = DEST_DIR / "communes.csv.lzma"
    return {
        "file_dep": [
            COMMUNES_CSV,
            EPCI_XLS,
            COMMUNES_POPULATION,
            COMMUNES_AD_POPULATION,
        ],
        "targets": [target],
        "actions": [
            (create_folder, [target.parent]),
            (
                traiter_communes,
                [
                    COMMUNES_CSV,
                    EPCI_XLS,
                    COMMUNES_POPULATION,
                    COMMUNES_AD_POPULATION,
                    target,
                ],
            ),
        ],
    }


def traiter_epci(epci_xls, dest):
    epci = (
        pd.read_excel(
            epci_xls,
            sheet_name="EPCI",
            skiprows=5,
            dtype={"EPCI": str},
            usecols=["EPCI", "LIBEPCI", "NATURE_EPCI"],
        )
        .rename(columns={"EPCI": "code", "LIBEPCI": "nom", "NATURE_EPCI": "type"})
        .iloc[:-1]  # éliminer le faux EPCI pas d'EPCI
    )

    with lzma.open(dest, "wt") as f:
        epci.to_csv(f, index=False)


def traiter_communes(
    communes_path, epci_path, communes_pop_path, communes_ad_pop_path, dest
):
    correspondances_epci = pd.read_excel(
        epci_path,
        sheet_name="Composition_communale",
        skiprows=5,
        dtype={"CODGEO": str, "EPCI": str},
        usecols=["CODGEO", "EPCI"],
    ).set_index("CODGEO")["EPCI"]
    # on élimine les "faux EPCI"
    correspondances_epci = correspondances_epci[correspondances_epci != "ZZZZZZZZZ"]

    communes_pop = (
        pd.read_csv(
            communes_pop_path,
            sep=";",
            usecols=["DEPCOM", "PMUN", "PCAP"],
            dtype={"DEPCOM": str},
        )
        .rename(columns={"PMUN": "population_municipale", "PCAP": "population_cap"})
        .set_index("DEPCOM")
    )

    communes_ad_pop = (
        pd.read_csv(
            communes_ad_pop_path,
            sep=";",
            usecols=["DEPCOM", "PMUN", "PCAP"],
            dtype={"DEPCOM": str},
        )
        .rename(columns={"PMUN": "population_municipale", "PCAP": "population_cap"})
        .set_index("DEPCOM")
    )

    communes = pd.read_csv(
        communes_path,
        dtype={"com": str, "dep": str, "comparent": str},
        usecols=["typecom", "com", "dep", "tncc", "nccenr", "comparent"],
    ).rename(
        columns={
            "com": "code",
            "dep": "code_departement",
            "typecom": "type",
            "nccenr": "nom",
            "tncc": "type_nom",
            "comparent": "commune_parent",
            "EPCI": "epci",
        }
    )

    with lzma.open(dest, "wt") as f:
        pd.concat(
            [
                communes.loc[communes.type == "COM"]
                .join(correspondances_epci, on=["code"])
                .join(communes_pop, on=["code"]),
                communes.loc[communes.type != "COM"].join(communes_ad_pop, on=["code"]),
            ],
            ignore_index=True,
        ).convert_dtypes().to_csv(f, index=False)
