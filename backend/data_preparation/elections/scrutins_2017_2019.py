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
    "Libellé de la commune": "commune_libelle",
    "Code du b.vote": "bureau",
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
    "N°Liste": "numero_liste",  # N°Liste
    "Sexe": "sexe",
    "Nom": "nom",
    "Prénom": "prenom",
    "Nuance": "nuance",
    "Libellé Abrégé Liste": "liste_court",  # Libellé Abrégé Liste
    "Libellé Etendu Liste": "liste_long",  # Libellé Etendu Liste
    "Nom Tête de Liste": "tete_liste",  # Nom Tête de Liste
    "Voix": "voix",
    "% Voix/Ins": None,
    "% Voix/Exp": None,
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


def clean_results(src, targets):
    targets = targets[0]
    with open(src, "r", encoding="latin1", newline="") as in_file:
        r = csv.reader(in_file, delimiter=";")

        headers = next(r)

        for h in headers:
            if h not in fixed_headers and h not in repeated_headers:
                raise RuntimeError(f"Champ '{h}' inconnu dans {src}")

        global_fields = [h for h in headers if h in fixed_headers]
        repeated_fields = [h for h in headers if h in repeated_headers]

        fields = [
            fixed_headers[field]
            for field in global_fields
            if fixed_headers.get(field) is not None
        ] + [
            repeated_headers[field]
            for field in repeated_fields
            if repeated_headers.get(field) is not None
        ]

        global_indices = [
            i for i, f in enumerate(global_fields) if fixed_headers[f] is not None
        ]
        repeated_indices = [
            i for i, f in enumerate(repeated_fields) if repeated_headers[f] is not None
        ]

        def all_lines():
            for line in r:
                global_values = [line[i] for i in global_indices]
                for i in range(len(global_fields), len(line), len(repeated_fields)):
                    candidate_values = [line[i + j] for j in repeated_indices]
                    yield (global_values + candidate_values)

        df = pd.DataFrame(list(all_lines()), columns=fields)

        for field, transform in transforms.items():
            if field in df.columns:
                df[field] = df[field].astype(transform)

        df.to_feather(targets)
