import pandas as pd

__all__ = ["task_traiter_epci", "task_traiter_communes"]

from backend import PREPARE_DIR

INSEE_DIR = PREPARE_DIR / "insee"
COG_DIR = INSEE_DIR / "cog"

EPCI_XLS = INSEE_DIR / "intercommunalite" / "epci.xls"
COMMUNES_COG = COG_DIR / "communes.csv"

EPCI_CSV = INSEE_DIR / "epci.csv"

COMMUNES_CSV = INSEE_DIR / "communes.csv"

COMMUNES_POPULATION = INSEE_DIR / "population" / "Communes.csv"
COMMUNES_AD_POPULATION = (
    INSEE_DIR / "population" / "Communes_associees_ou_deleguees.csv"
)


def task_traiter_epci():
    return {
        "file_dep": [EPCI_XLS],
        "targets": [EPCI_CSV],
        "actions": [(traiter_epci, [EPCI_XLS, EPCI_CSV]),],
    }


def task_traiter_communes():
    return {
        "file_dep": [
            COMMUNES_COG,
            EPCI_XLS,
            COMMUNES_POPULATION,
            COMMUNES_AD_POPULATION,
        ],
        "targets": [COMMUNES_CSV],
        "actions": [
            (
                traiter_communes,
                [
                    COMMUNES_COG,
                    EPCI_XLS,
                    COMMUNES_POPULATION,
                    COMMUNES_AD_POPULATION,
                    COMMUNES_CSV,
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

    with open(dest, "w") as f:
        epci.to_csv(f, index=False)


def traiter_communes(
    communes_cog_path, epci_path, communes_pop_path, communes_ad_pop_path, dest
):
    correspondances_epci = (
        pd.read_excel(
            epci_path,
            sheet_name="Composition_communale",
            skiprows=5,
            dtype={"CODGEO": str, "EPCI": str},
            usecols=["CODGEO", "EPCI"],
        )
        .set_index("CODGEO")["EPCI"]
        .str.strip()
    )
    # on élimine les "faux EPCI"
    correspondances_epci = correspondances_epci[correspondances_epci != "ZZZZZZZZZ"]
    correspondances_epci.name = "epci"
    correspondances_epci.index = correspondances_epci.index.str.strip()

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
    communes_pop.index = communes_pop.index.str.strip()

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
    communes_ad_pop.index = communes_ad_pop.index.str.strip()

    communes = pd.read_csv(
        communes_cog_path,
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
        }
    )

    communes["code_departement"] = (
        communes["code"]
        .str.slice(0, 3)
        .where(
            communes["code"].str.startswith("97") & (communes["type"] == "COM"),
            communes["code"].str.slice(0, 2).where(communes["type"] == "COM"),
        )
    )

    for c in ["type", "code", "nom", "commune_parent"]:
        communes[c] = communes[c].str.strip()

    res = pd.concat(
        [
            communes.loc[communes.type.isin(["COM", "ARM"])]
            .join(correspondances_epci, on=["code"])
            .join(communes_pop, on=["code"])
            .sort_values(["type", "code"], ascending=[False, True]),
            communes.loc[~communes.type.isin(["COM", "ARM"])]
            .join(communes_ad_pop, on=["code"])
            .sort_values(["type", "code"]),
        ],
        ignore_index=True,
    ).convert_dtypes()

    villes_arms = [("13055", "132"), ("69123", "693"), ("75056", "751")]

    for code_ville, prefix_arm in villes_arms:
        res.loc[
            res["code"] == code_ville, ["population_municipale", "population_cap"]
        ] = list(
            res.loc[
                res["code"].str.startswith(prefix_arm),
                ["population_municipale", "population_cap"],
            ].sum(axis=0)
        )

    res.to_csv(dest, index=False)
