from pathlib import Path

import pandas as pd

entetes = [
    "numero_tour",  # -- Champ 1  : N° tour
    "departement",  # -- Champ 2  : Code département
    "commune",  # -- Champ 3  : Code commune
    "commune_libelle",  # -- Champ 4  : Nom de la commune
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
    "numero_tour": int,
    "departement": "category",
    "numero_panneau": int,
    "nuance": "category",
    "inscrits": int,
    "votants": int,
    "exprimes": int,
    "voix": int,
}

types_par_colonne = {
    **{h: str for h in entetes},
    **{"inscrits": int, "votants": int, "exprimes": int, "voix": int},
}


def clean_results(src, targets):
    if isinstance(targets, (str, Path)):
        targets = [targets]

    # trouver la première ligne
    with open(src, "r", encoding="latin1") as f:
        for i, line in enumerate(f):
            if ";" in line:
                break

    df = pd.read_csv(
        src,
        sep=";",
        skiprows=i,
        names=entetes,
        dtype=types_par_colonne,
        encoding="latin1",
    )

    for field, transform in transforms.items():
        df[field] = df[field].astype(transform)

    if df.nom.nunique() < 500:
        df["nom"] = df["nom"].astype("category")
        df["prenom"] = df["prenom"].astype("category")

    for tour, dest in zip(sorted(df["numero_tour"].unique()), targets):
        df[df["numero_tour"] == tour].reset_index(drop=True).to_feather(dest)
