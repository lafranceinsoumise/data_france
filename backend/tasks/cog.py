import dataclasses
from enum import Enum
import pandas as pd

__all__ = ["task_traiter_epci", "task_traiter_communes", "task_traiter_cantons"]

from sources import PREPARE_DIR, SOURCES
from data_france.data import VILLES_PLM
from operator import attrgetter
from datetime import datetime
from typing import List


ANNEE_COG = "2024"

COMMUNE_TYPE_ORDERING = ["COM", "ARM", "COMA", "COMD", "SRM", None]


INSEE_DIR = PREPARE_DIR / "insee"
COG_DIR = INSEE_DIR / "cog"

EPCI_XLS = INSEE_DIR / "intercommunalite" / "epci.xlsx"
EVENEMENTS_COG = COG_DIR / f"v_mvt_commune_{ANNEE_COG}.csv"
COMMUNES_COG = COG_DIR / f"v_commune_{ANNEE_COG}.csv"
CANTONS_COG = COG_DIR / f"v_canton_{ANNEE_COG}.csv"
DEPARTEMENTS_COG = COG_DIR / f"v_departement_{ANNEE_COG}.csv"
REGIONS_COG = COG_DIR / f"v_region_{ANNEE_COG}.csv"
COLLECTIVITES_DEPARTEMENTALES_COG = COG_DIR / f"v_ctcd_{ANNEE_COG}.csv"

EPCI_CSV = INSEE_DIR / "epci.csv"
COMMUNES_CSV = INSEE_DIR / "communes.csv"
CANTONS_CSV = INSEE_DIR / "cantons.csv"
CORR_SOUS_COMMUNES = INSEE_DIR / "correspondances_sous_communes.csv"


COMMUNES_POPULATION = INSEE_DIR / "population" / "Communes.csv"
COMMUNES_AD_POPULATION = (
    INSEE_DIR / "population" / "Communes_associees_ou_deleguees.csv"
)


def task_traiter_epci():
    return {
        "file_dep": [EPCI_XLS],
        "targets": [EPCI_CSV],
        "actions": [
            (traiter_epci, [EPCI_XLS, EPCI_CSV]),
        ],
    }


def task_traiter_communes():
    return {
        "file_dep": [
            EVENEMENTS_COG,
            COMMUNES_COG,
            EPCI_XLS,
            COMMUNES_POPULATION,
            COMMUNES_AD_POPULATION,
        ],
        "targets": [COMMUNES_CSV, CORR_SOUS_COMMUNES],
        "actions": [
            (
                traiter_communes,
                (),
                {
                    "communes_cog_path": COMMUNES_COG,
                    "epci_path": EPCI_XLS,
                    "communes_pop_path": COMMUNES_POPULATION,
                    "communes_ad_pop_path": COMMUNES_AD_POPULATION,
                    "evenements_path": EVENEMENTS_COG,
                    "dest": COMMUNES_CSV,
                    "corr_sous_communes": CORR_SOUS_COMMUNES,
                },
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
    print(f"EPCI EXCEL {epci_xls}")
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
    communes_cog_path,
    epci_path,
    communes_pop_path,
    communes_ad_pop_path,
    evenements_path,
    dest,
    corr_sous_communes,
):
    correspondances_epci = (
        pd.read_excel(
            epci_path,
            sheet_name="Composition_communale",
            skiprows=5,
            dtype={"CODGEO": str, "EPCI": str},
            usecols=["CODGEO", "EPCI"],
            engine="openpyxl",
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

    gerer_changements_communes_population(
        communes_pop, communes_ad_pop, evenements_path
    )

    communes = pd.read_csv(
        communes_cog_path,
        dtype={"COM": str, "DEP": str, "COMPARENT": str},
        usecols=["TYPECOM", "COM", "TNCC", "NCCENR", "COMPARENT"],
    ).rename(
        columns={
            "COM": "code",
            "TYPECOM": "type",
            "NCCENR": "nom",
            "TNCC": "type_nom",
            "COMPARENT": "commune_parent",
        }
    )

    # table de correspondances pour les communes déléguées et associées
    communes[communes["commune_parent"].notnull()].to_csv(
        corr_sous_communes, index=False
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

    arms = communes_et_arrs.loc[
        (communes_et_arrs.type == "ARM")
        & communes_et_arrs.code.isin([v.arrondissements for v in VILLES_PLM])
    ].set_index("code")

    secteurs_municipaux = pd.DataFrame(
        [
            {
                "code": secteur.code,
                "type": "SRM",
                "nom": secteur.nom,
                "type_nom": ville.type_nom,
                "commune_parent": ville.code,
                "population_municipale": arms.loc[
                    arms.index.isin(secteur.arrondissements),
                    "population_municipale",
                ].sum(),
                "population_cap": arms.loc[
                    arms.index.isin(secteur.arrondissements),
                    "population_cap",
                ].sum(),
            }
            for ville in VILLES_PLM
            for secteur in ville.secteurs
        ]
    )

    res = pd.concat(
        [communes_et_arrs, anciennes_communes, secteurs_municipaux],
        ignore_index=True,
    ).convert_dtypes()

    for ville in VILLES_PLM:
        res.loc[
            res["code"] == ville.code, ["population_municipale", "population_cap"]
        ] = list(
            res.loc[
                res["code"].isin(ville.arrondissements),
                ["population_municipale", "population_cap"],
            ].sum(axis=0)
        )

    res.to_csv(dest, index=False)


def traiter_cantons(cantons_cog_path, dest):
    # on nomme nous-mêmes les colonnes parce que leurs noms dans le fichier sont incorrects en 2022
    cantons = pd.read_csv(
        cantons_cog_path,
        dtype={
            "code": str,
            "departement": str,
            "bureau_centralisateur": str,
            "composition": pd.UInt32Dtype(),
        },
        names=[
            "code",
            "departement",
            "_region",
            "composition",
            "bureau_centralisateur",
            "type_nom",
            "_ncc",
            "nom",
            "_libelle",
            "type",
        ],
        header=0,
    )

    cantons[[c for c in cantons.columns if c[0] != "_"]].to_csv(dest, index=False)


def importer_evenements_communes(path):
    evenements = pd.read_csv(path)
    date_eff = evenements.DATE_EFF.str.extract(
        r"^(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{2})$"
    )
    date_eff["year"] = (
        pd.Series("19", index=date_eff.index).where(date_eff.year >= "25", "20")
        + date_eff.year
    )
    evenements["DATE_EFF"] = pd.to_datetime(date_eff)
    return evenements[evenements.DATE_EFF > SOURCES.insee.population.date]


def gerer_changements_communes_population(population, population_ad, evenements_path):
    """Gérer l'évolution des communes depuis la date d'arrêt de la population

    Il faut à la fois gérer différents types d'événements, avec des impacts
    différents sur le comptage des populations, et en même temps les gérer
    séquentiellement, puisque les résultats d'une action peuvent influer sur une
    action ultérieure.

    La solution retenue est de générer séparément des actions, qui sont de trois
    types différents (rétablissement de communes, fusions de communes, et
    changements de code), et générées selon des règles différentes (nécessité de
    grouper les lignes correspondant à un même changement ou non), puis ensuite
    de les exécuter une par une, dans l'ordre chronologique.
    """

    evenements = importer_evenements_communes(evenements_path)

    actions = sorted(
        (
            a
            for kls in (ActionRetablissement, ActionFusion, ActionChangementCode)
            for a in kls.depuis_evenements(evenements)
        ),
        key=attrgetter("date"),
    )

    for a in actions:
        a.maj_population(population, population_ad)


class MOD(Enum):
    CHANGEMENT_NOM = 10
    CREATION = 20
    RETABLISSEMENT = 21
    SUPPRESSION = 30
    FUSION_SIMPLE = 31
    CREATION_COMMUNE_NOUVELLE = 32
    FUSION_ASSOCIATION = 33
    TRANSFORMATION_FUSION_SIMPLE = 34
    SUPPRESSION_COMMUNE_DELEGUEE = 35
    CHANGEMENT_CODE_DEPARTEMENT = 41
    CHANGEMENT_CODE_CHEF_LIEU = 50
    TRANSFORMATION_COMMUNE_ASSOCIEE_DELEGUEE = 70


@dataclasses.dataclass
class ActionRetablissement:
    """Représente le rétablissement d'une commune auparavant supprimée par une fusion

    On ne crée une telle action que dans le cas où la commune avait survécu
    comme commune déléguée ou associée.
    """

    date: datetime
    com_av: str
    com_ap: str

    mods = [MOD.CREATION, MOD.RETABLISSEMENT]

    def maj_population(self, pop_com, pop_ad):
        """Prend en compte le rétablissement dans les tables de population

        On reporte la valeur de population enregistrée depuis le tableau de
        sous-communes vers celui des communes de plein exercice.
        """
        pop_com.loc[self.com_ap] = pop_ad.loc[self.com_av]

    @classmethod
    def depuis_evenements(cls, evenements):
        """Génère les actions de rétablissement à partir du fichier evenements

        Il n'y a pas de problème à générer ligne par ligne de telles actions.
        """
        return [
            cls(t.DATE_EFF, t.COM_AV, t.COM_AP)
            for t in evenements[
                evenements.MOD.isin(m.value for m in cls.mods)
                & evenements.TYPECOM_AV.isin(["COMA", "COMD"])
            ].itertuples()
        ]


@dataclasses.dataclass
class ActionFusion:
    """Représente la fusion de plusieurs communes.

    Cette classe ne modèle pas la conservation ou non des communes d'origine
    comme communes déléguées ou associées ; on n'a en réalité pas besoin de
    cette information.
    """

    date: datetime
    com_av: List[str]
    com_ap: str

    mods = [MOD.FUSION_ASSOCIATION, MOD.FUSION_SIMPLE, MOD.CREATION_COMMUNE_NOUVELLE]

    def maj_population(self, pop_com, pop_ad):
        """Prend en compte la fusion dans les tables de population

        Une fusion correspond à la transformation simultanée des anciennes
        communes de plein exercice en communes déléguées ou associées, et à la
        création d'une commune nouvelle (qui reprend généralement le code d'une
        des communes précédemment existantes.

        En terme de population, il faut donc d'abord recopier les populations
        depuis le tableau des communes de plein exercice vers celui des
        sous-communes, puis faire la somme des populations des communes
        préexistantes et l'inscrire au code de la commune nouvelle.
        """

        for c in self.com_av:
            pop_ad.loc[c] = pop_com.loc[c]

        pop_com.loc[self.com_ap] = pop_com.loc[self.com_av].sum(axis=0)

    @classmethod
    def depuis_evenements(cls, evenements):
        """Génère les actions de fusion à partir du fichier evenements

        Pour pouvoir prendre en compte que la transformation en communes
        déléguées/associées et la création de la commune nouvelle sont
        simultanées, il faut faire un group by sur la table des événements pour
        générer une action unique pour toute la fusion. Contrairement à un
        rétablissement, il n'est pas possible de faire ligne par ligne.
        """
        return [
            cls(date, list(groupe["COM_AV"]), com_ap)
            for (date, com_ap), groupe in evenements[
                evenements.MOD.isin(m.value for m in cls.mods)
                & (evenements.TYPECOM_AP == "COM")
            ].groupby(["DATE_EFF", "COM_AP"])
        ]


@dataclasses.dataclass
class ActionChangementCode:
    """Représente le changement de code d'une commune ou sous-commune"""

    date: datetime
    type: str
    com_av: str
    com_ap: str

    mods = [MOD.CHANGEMENT_CODE_CHEF_LIEU, MOD.CHANGEMENT_CODE_DEPARTEMENT]

    def maj_population(self, pop_com, pop_ad):
        """Prend en compte le changement de code dans les tables de population"""
        if self.type == "COM":
            pop_com.loc[self.com_ap] = pop_com.loc[self.com_av]
        else:
            pop_ad.loc[self.com_ap] = pop_ad.loc[self.com_av]

    @classmethod
    def depuis_evenements(cls, evenements):
        """Génère les actions de changement de code à partir du fichier evenemlents"""
        return [
            cls(
                date=t.DATE_EFF,
                type=t.TYPECOM_AV,
                com_av=t.COM_AV,
                com_ap=t.COM_AP,
            )
            for t in evenements[
                evenements.MOD.isin(m.value for m in cls.mods)
            ].itertuples()
        ]
