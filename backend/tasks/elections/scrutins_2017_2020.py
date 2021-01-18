"""
Ce fichier comprend la routine permettant de nettoyer les fichiers électoraux des scrutins de 2017 et 2019
"""

import csv
import pandas as pd

fixed_headers = {
    "Code du département": "departement",
    "Libellé du département": None,
    "Code de la circonscription": "circonscription",
    "Libellé de la circonscription": None,
    "Code de la commune": "commune",
    "Libellé de la commune": None,
    "Code du b.vote": "bureau",
    "Code B.Vote": "bureau",
    "Inscrits": "inscrits",
    "Abstentions": None,
    "% Abs/Ins": None,
    "Votants": "votants",
    "% Vot/Ins": None,
    "Blancs": "blancs",
    "% Blancs/Ins": None,
    "% Blancs/Vot": None,
    "Nuls": None,
    "% Nuls/Ins": None,
    "% Nuls/Vot": None,
    "Exprimés": "exprimes",
    "% Exp/Ins": None,
    "% Exp/Vot": None,
}
"""
Comment renommer les entêtes ?
"""

repeated_headers = {
    "N°Panneau": "numero_panneau",
    "N.Pan.": "numero_panneau",
    "N°Liste": "numero_panneau",  # N°Liste
    "Sexe": "sexe",
    "Nom": "nom",
    "Prénom": "prenom",
    "Nuance": "nuance",
    "Code Nuance": "nuance",
    "Libellé Abrégé Liste": "liste_court",  # Libellé Abrégé Liste
    "Libellé Etendu Liste": "liste_long",  # Libellé Etendu Liste
    "Liste": "liste_long",
    "Nom Tête de Liste": "tete_liste",  # Nom Tête de Liste
    "Voix": "voix",
    "% Voix/Ins": None,
    "% Voix/Exp": None,
    "Sièges / Elu": None,
    "Sièges Secteur": None,
    "Sièges CC": None,
}


transforms = {
    "circonscription": int,
    "inscrits": int,
    "votants": int,
    "exprimes": int,
    "blancs": int,
    "voix": int,
    "sexe": "category",
    "numero_panneau": int,
    "numero_liste": int,
    "nuance": "category",
    "nom": "category",
    "prenom": "category",
    "liste_court": "category",
    "liste_long": "category",
    "tete_liste": "category",
}

identifiants = ["departement", "commune", "bureau"]
population = ["inscrits", "votants", "exprimes"]
par_candidat = [
    "numero_panneau",
    "nuance",
    "nom",
    "prenom",
    "liste_court",
    "liste_long",
    "voix",
]


def clean_results(src, base_filenames, delimiter=";"):
    base_filename = base_filenames[0]
    with open(src, "r", encoding="latin1", newline="") as in_file:
        r = csv.reader(in_file, delimiter=delimiter)

        headers = next(r)

        for h in headers:
            if h not in fixed_headers and h not in repeated_headers:
                raise RuntimeError(f"Champ '{h}' inconnu dans {src}")

        common_fields = [h for h in headers if h in fixed_headers]

        # attention aux répétitions
        candidate_specific_fields = []
        for h in headers:
            if h in repeated_headers and h not in candidate_specific_fields:
                candidate_specific_fields.append(h)

        fields = [
            fixed_headers[field]
            for field in common_fields
            if fixed_headers.get(field) is not None
        ] + [
            repeated_headers[field]
            for field in candidate_specific_fields
            if repeated_headers.get(field) is not None
        ]

        common_indices = [
            i for i, f in enumerate(common_fields) if fixed_headers[f] is not None
        ]
        candidate_specific_indices = [
            i
            for i, f in enumerate(candidate_specific_fields)
            if repeated_headers[f] is not None
        ]

        all_entries = []

        for line in r:
            common_values = [line[i] for i in common_indices]
            for candidate_offset in range(
                len(common_fields), len(line), len(candidate_specific_fields)
            ):
                candidate_specific_values = [
                    line[candidate_offset + j] for j in candidate_specific_indices
                ]
                if candidate_specific_values[-1] == "":
                    break
                all_entries.append(common_values + candidate_specific_values)

        df = pd.DataFrame(all_entries, columns=fields)

        for field, transform in transforms.items():
            try:
                if field in df.columns:
                    df[field] = df[field].astype(transform)
            except ValueError as e:
                print(f"global_fields: {common_fields!r}")
                print(f"repeated_fields: {candidate_specific_fields!r}")
                raise ValueError(f"Echec transformation {transform} sur {field}")

        df["code"] = (
            df["departement"].str.zfill(2)
            + df["commune"].str.zfill(3)
            + "-"
            + df["bureau"].str.zfill(4)
        )

        df.groupby(["code"]).agg(
            {f: "first" for f in population}
        ).reset_index().to_feather(f"{base_filename}-pop.feather")

        df[["code", *[c for c in par_candidat if c in df.columns]]].to_feather(
            f"{base_filename}-votes.feather"
        )
