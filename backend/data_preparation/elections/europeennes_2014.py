import pandas as pd

entetes = [
    "numero_tour",
    "departement",
    "commune",
    "commune_libelle",
    "bureau",
    "inscrits",
    "votants",
    "exprimes",
    "numero_panneau",
    "nom",
    "prenom",
    "nuance",
    "voix",
]

types_par_colonne = {
    **{h: str for h in entetes},
    **{"inscrits": int, "votants": int, "exprimes": int, "voix": int},
}


def clean_results(src, dest):
    df = pd.read_csv(
        src,
        sep=";",
        skiprows=16,
        names=entetes,
        dtype=types_par_colonne,
        encoding="latin1",
        usecols=entetes[1:],
    )

    df.to_csv(dest, index=False)

    return {"columns": list(df.columns)}
