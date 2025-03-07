import pandas as pd

champs = {
    "numero_tour": None,  # -- Champ 1  : N° tour
    "region": None,
    "departement": str,  # -- Champ 2  : Code département
    "arrondissement": None,
    "circonscription": None,
    "canton": None,
    "commune": str,  # -- Champ 3  : Code commune
    "inscrits_reference": None,
    "nom_commune": None,
    "bureau": str,  # -- Champ 5  : N° de bureau de vote
    "inscrits": int,  # -- Champ 6  : Inscrits
    "votants": int,  # -- Champ 7  : Votants
    "abstention": None,
    "exprimes": int,  # -- Champ 8  : Exprimé
    "reponse": str,
    "voix": int,  # -- Champ 13 : Nombre de voix
}

population = ["inscrits", "votants", "exprimes"]


def clean_results(src, base_filenames, delimiter, encoding="utf-8"):
    base_filename = base_filenames[0]
    with open(src, "r", encoding=encoding) as f:
        for i, line in enumerate(f):
            if delimiter in line:
                break

    df = pd.read_csv(
        src,
        sep=delimiter,
        skiprows=i,
        names=list(champs),
        header=None,
        usecols=[c for c, t in champs.items() if t is not None],
        dtype=champs,
        encoding="latin1",
    )

    df["reponse"] = df["reponse"].astype("category")

    df["code"] = (
        df["departement"].str.zfill(2)
        + df["commune"].str.zfill(3)
        + "-"
        + df["bureau"].str.zfill(4)
    )

    df[["code", *population]].groupby("code").agg(
        {f: "first" for f in population}
    ).reset_index().to_feather(f"{base_filename}-pop.feather")
    df[["code", "reponse", "voix"]].to_feather(f"{base_filename}-votes.feather")
