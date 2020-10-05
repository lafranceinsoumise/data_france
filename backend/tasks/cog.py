import pandas as pd

__all__ = ["task_traiter_epci", "task_traiter_communes", "task_traiter_cantons"]

from backend import PREPARE_DIR
from data_france.data import VILLES_PLM

INSEE_DIR = PREPARE_DIR / "insee"
COG_DIR = INSEE_DIR / "cog"

EPCI_XLS = INSEE_DIR / "intercommunalite" / "epci.xls"
COMMUNES_COG = COG_DIR / "communes.csv"
CANTONS_COG = COG_DIR / "cantons.csv"

EPCI_CSV = INSEE_DIR / "epci.csv"
COMMUNES_CSV = INSEE_DIR / "communes.csv"
CANTONS_CSV = INSEE_DIR / "cantons.csv"


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


def task_traiter_cantons():
    return {
        "file_dep": [CANTONS_COG],
        "targets": [CANTONS_CSV],
        "actions": [(traiter_cantons, [CANTONS_COG, CANTONS_CSV])],
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
        usecols=["typecom", "com", "tncc", "nccenr", "comparent"],
    ).rename(
        columns={
            "com": "code",
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

    communes_et_arrs = (
        communes.loc[communes.type.isin(["COM", "ARM"])]
        .join(correspondances_epci, on=["code"])
        .join(communes_pop, on=["code"])
        .sort_values(["type", "code"], ascending=[False, True])
    )

    anciennes_communes = (
        communes.loc[~communes.type.isin(["COM", "ARM"])]
        .join(communes_ad_pop, on=["code"])
        .sort_values(["type", "code"])
    )

    plms = communes_et_arrs.loc[
        (communes_et_arrs.type == "COM")
        & communes_et_arrs.code.isin([v["code"] for v in VILLES_PLM])
    ].set_index("code")
    arms = communes_et_arrs.loc[
        (communes_et_arrs.type == "ARM")
        & communes_et_arrs.code.str.match(
            "|".join(v["prefixe_arm"] for v in VILLES_PLM)
        )
    ].set_index("code")

    secteurs_municipaux = pd.DataFrame(
        [
            {
                "code": f"{ville['code']}SR{sec:02d}",
                "type": "SRM",
                "nom": f"{plms.loc[ville['code'], 'nom']} {sec}{'er' if sec==1 else 'e'} secteur électoral",
                "type_nom": plms.loc[ville["code"], "type_nom"],
                "commune_parent": ville["code"],
                "population_municipale": arms.loc[
                    arms.index.isin([f"{ville['prefixe_arm']}{arr}" for arr in arrs]),
                    "population_municipale",
                ].sum(),
                "population_cap": arms.loc[
                    arms.index.isin([f"{ville['prefixe_arm']}{arr}" for arr in arrs]),
                    "population_cap",
                ].sum(),
            }
            for ville in VILLES_PLM
            for sec, arrs in ville["secteurs"].items()
        ]
    )

    res = pd.concat(
        [communes_et_arrs, anciennes_communes, secteurs_municipaux], ignore_index=True,
    ).convert_dtypes()

    for ville in VILLES_PLM:
        res.loc[
            res["code"] == ville["code"], ["population_municipale", "population_cap"]
        ] = list(
            res.loc[
                res["code"].str.startswith(ville["prefixe_arm"]),
                ["population_municipale", "population_cap"],
            ].sum(axis=0)
        )

    res.to_csv(dest, index=False)


def traiter_cantons(cantons_cog_path, dest):
    cantons = pd.read_csv(
        cantons_cog_path,
        dtype={"can": str, "dep": str, "burcentral": str, "compct": pd.UInt32Dtype()},
        usecols=["can", "typect", "compct", "nccenr", "tncc", "dep", "burcentral",],
    )

    # La commune d'Azé (53014) est maintenant une commune déléguée de Château-Gontier-sur-Mayenne
    cantons.loc[cantons["burcentral"] == "53014", "burcentral"] = "53062"

    cantons.rename(
        columns={
            "can": "code",
            "typect": "type",
            "compct": "composition",
            "nccenr": "nom",
            "tncc": "type_nom",
            "dep": "departement",
            "burcentral": "bureau_centralisateur",
        }
    ).to_csv(dest, index=False)
