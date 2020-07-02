from pathlib import Path

import pandas as pd

entetes = {
    "numero_tour": True,  # -- Champ 1  : N° tour
    "departement": True,  # -- Champ 2  : Code département
    "commune": True,  # -- Champ 3  : Code commune
    "commune_libelle": False,  # -- Champ 4  : Nom de la commune
    "bureau": True,  # -- Champ 5  : N° de bureau de vote
    "inscrits": True,  # -- Champ 6  : Inscrits
    "votants": True,  # -- Champ 7  : Votants
    "exprimes": True,  # -- Champ 8  : Exprimés
    "numero_panneau": True,  # -- Champ 9  : N° de dépôt de la liste
    "nom": True,  # -- Champ 10 : Nom du candidat tête de liste
    "prenom": True,  # -- Champ 11 : Prénom du candidat  tête de liste
    "nuance": True,  # -- Champ 12 : Code nuance de la liste
    "voix": True,  # -- Champ 13 : Nombre de voix
}


transforms = {
    "numero_tour": int,
    "numero_panneau": int,
    "nuance": "category",
    "inscrits": int,
    "votants": int,
    "exprimes": int,
    "voix": int,
}

identifiants = ["departement", "commune", "bureau"]
population = ["inscrits", "votants", "exprimes"]
par_candidat = ["numero_panneau", "nom", "prenom", "nuance", "voix"]


types_par_colonne = {
    **{h: str for h in entetes},
    **{"inscrits": int, "votants": int, "exprimes": int, "voix": int},
}


def clean_results(src, base_filenames, delimiter):
    if isinstance(base_filenames, (str, Path)):
        base_filenames = [base_filenames]

    # trouver la première ligne
    with open(src, "r", encoding="latin1") as f:
        for i, line in enumerate(f):
            if delimiter in line:
                break

    df = pd.read_csv(
        src,
        sep=delimiter,
        skiprows=i,
        names=entetes,
        usecols=[e for e, b in entetes.items() if b],
        dtype=types_par_colonne,
        encoding="latin1",
    )

    for field, transform in transforms.items():
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
        df.loc[df["numero_tour"] == tour, ["code", *population]].groupby("code").agg(
            {f: "first" for f in population}
        ).reset_index().to_feather(f"{base_filename}-pop.feather")
        df.loc[df["numero_tour"] == tour, ["code", *par_candidat]].reset_index(
            drop=True
        ).to_feather(f"{base_filename}-votes.feather")
