from itertools import chain
from pathlib import Path

import pandas as pd

partie_commune = [
    "numero_tour",  # -- Champ 1  : N° tour
    "departement",  # -- Champ 2  : Code département
    "commune",  # -- Champ 3  : Code commune
    "nom_commune",
    "circonscription",
    "canton",
]

partie_bureau = [
    "bureau",  # -- Champ 5  : N° de bureau de vote
    "inscrits",  # -- Champ 6  : Inscrits
    "votants",  # -- Champ 7  : Votants
    "exprimes",  # -- Champ 8  : Exprimés
    "numero_panneau",  # -- Champ 9  : N° de dépôt de la liste
    "nom",  # -- Champ 10 : Nom du candidat tête de liste
    "prenom",  # -- Champ 11 : Prénom du candidat  tête de liste
    "nuance",  # -- Champ 12 : Code nuance de la liste
    "voix",  # -- Champ 13 : Nombre de voix
]


transforms = {
    "circonscription": "int",
    "numero_tour": "int",
    "numero_panneau": "int",
    "nuance": "category",
    "inscrits": "int",
    "votants": "int",
    "exprimes": "int",
    "voix": "int",
}

population = ["circonscription", "canton", "inscrits", "votants", "exprimes"]
par_candidat = ["numero_panneau", "nom", "prenom", "nuance", "voix"]


types_par_colonne = {
    **{h: str for h in chain(partie_commune, partie_bureau)},
    **{"inscrits": int, "votants": int, "exprimes": int, "voix": int},
}


def clean_results(
    src,
    base_filenames,
    delimiter,
    encoding="latin1"
):
    if isinstance(base_filenames, (str, Path)):
        base_filenames = [base_filenames]

    # trouver la première ligne
    with open(src, "r", encoding=encoding) as f:
        for i, line in enumerate(f):
            if delimiter in line:
                nb_champs = len(line.split(delimiter))
                break

    nb_communs = nb_champs - len(partie_bureau)  # type: ignore[reportUnboundVariable]
    names = partie_commune[:nb_communs] + partie_bureau

    df: pd.DataFrame = pd.read_csv(
        src,
        sep=delimiter,
        skiprows=i,  # type: ignore[reportUnboundVariable]
        names=names,  # type: ignore
        header=None,  # type: ignore
        dtype=types_par_colonne,  # type: ignore
        encoding="latin1",
    )

    for field, transform in transforms.items():
        if field in df.columns:
            df[field] = df[field].astype(transform)

    if df.nom.nunique() < 1000:
        df["nom"] = df["nom"].astype("category")
        df["prenom"] = df["prenom"].astype("category")

    df["code"] = (
        df["departement"].str.zfill(2)
        + df["commune"].str.zfill(3)
        + "-"
        + df["bureau"].str.zfill(4)
    )

    for tour, base_filename in zip(sorted(df["numero_tour"].unique()), base_filenames):
        df.loc[df["numero_tour"] == tour].groupby("code").agg(
            {f: "first" for f in population if f in df.columns}
        ).reset_index().to_feather(f"{base_filename}-pop.feather")
        df.loc[df["numero_tour"] == tour, ["code", *par_candidat]].reset_index(
            drop=True
        ).to_feather(f"{base_filename}-votes.feather")
