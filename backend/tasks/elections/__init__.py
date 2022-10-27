from pathlib import Path

from doit.tools import create_folder

from sources import SOURCE_DIR, PREPARE_DIR, SOURCES
from .referendum2005 import clean_results as clean_results_2005
from .scrutins_2014 import clean_results as clean_results_2014
from .scrutins_2017_2020 import clean_results as clean_results_post_2017

__all__ = [
    "task_preparer",
    "task_corriger_referendum_2005_caen",
    "task_corriger_municipales_2020_tour_1",
    "task_election_csv",
]


sans_deuxieme_tour = {"europeennes", "referendums"}

RESULTATS_DIR = PREPARE_DIR / "interieur" / "resultats_electoraux"

SELECTION_CANDIDAT = {
    ("2012", "presidentielles"): "nom",
    ("2012", "legislatives"): "nuance",
    ("2017", "presidentielles"): "nom",
    ("2017", "legislatives"): "nuance",
    ("2019", "europeennes"): "liste_court",
    ("2021", "departementales"): "nuance",
    ("2021", "regionales"): "nuance",
    ("2022", "presidentielles"): "nom",
    ("2022", "legislatives"): "nuance",
}


def task_corriger_municipales_2020_tour_1():
    """Le fichier du premier tour des municipales 2020 présente des erreurs de décalage de cellules

    Cette tâche corrige ces erreurs en utilisant sed.
    """
    source_file = (
        SOURCE_DIR
        / "interieur"
        / "resultats_electoraux"
        / "municipales"
        / "2020"
        / "tour1.txt"
    )

    return {
        "file_dep": [source_file],
        "targets": [source_file.with_suffix(".csv")],
        "actions": [
            "sed '13751,13752s/ \t / / ; 16921s/ \t / /' < {dependencies} > {targets}"
        ],
    }


def task_corriger_referendum_2005_caen():
    """Corrige les lignes correspondant aux résultats de Caen pour le référendum sur le TCE de 2005"""
    source = (
        SOURCE_DIR / SOURCES.interieur.resultats_electoraux.referendums["2005"].filename
    )

    return {
        "file_dep": [source],
        "targets": [source.with_suffix(".csv")],
        "actions": [
            "sed -E '15337,15452s/;Caen;([[:digit:]]{{2}});([[:digit:]])/;Caen;\10\2/' "
            "< {dependencies} > {targets}",
        ],
    }


def task_preparer():
    """Extrait et formate les résultats des élections françaises de façon uniforme."""
    for source in SOURCES.interieur.resultats_electoraux:
        election, annee = source.path.parts[2:4]
        tour = int(source.path.parts[4][-1]) if len(source.path.parts) == 5 else None

        src = SOURCE_DIR / source.filename
        if source.corrected:
            src = src.with_suffix(source.corrected)

        if tour:
            base_filenames = [RESULTATS_DIR / f"{annee}-{election}-{tour}"]
        elif election in sans_deuxieme_tour:
            base_filenames = [RESULTATS_DIR / f"{annee}-{election}"]
        else:
            base_filenames = [
                RESULTATS_DIR / f"{annee}-{election}-{tour}" for tour in [1, 2]
            ]

        targets = [
            f.with_name(f.name + ext)
            for f in base_filenames
            for ext in ["-pop.feather", "-votes.feather"]
        ]

        if election == "referendums" and int(annee) == 2005:
            func = clean_results_2005
        elif int(annee) >= 2017:
            func = clean_results_post_2017
        else:
            func = clean_results_2014

        yield {
            "name": "/".join(source.path.parts[2:]),
            "targets": targets,
            "file_dep": [src],
            "actions": [
                *[(create_folder, [Path(t).parent]) for t in base_filenames],
                (
                    func,
                    [],
                    {
                        "src": src,
                        "base_filenames": base_filenames,
                        "delimiter": source.delimiter,
                    },
                ),
            ],
        }


def task_election_csv():
    """Exporte les résultats électoraux au format CSV."""
    for source in SOURCES.interieur.resultats_electoraux:
        election, annee = source.path.parts[2:4]

        tour = int(source.path.parts[4][-1]) if len(source.path.parts) == 5 else None

        if tour:
            tours = [tour]
        elif election in sans_deuxieme_tour:
            tours = [None]
        else:
            tours = [1, 2]

        for tour in tours:
            if tour is None:
                name = f"{election}/{annee}"
                base = RESULTATS_DIR / f"{annee}-{election}"
            else:
                name = f"{election}/{annee}/tour{tour}"
                base = RESULTATS_DIR / f"{annee}-{election}-{tour}"

            sources = [
                base.with_name(f"{base.name}-pop.feather"),
                base.with_name(f"{base.name}-votes.feather"),
            ]

            target = base.with_suffix(".csv")
            grouper_candidat = SELECTION_CANDIDAT.get((annee, election))
            if grouper_candidat is None:
                continue

            yield {
                "name": name,
                "file_dep": sources,
                "targets": [target],
                "actions": [(feather_to_csv, (*sources, grouper_candidat, target), {})],
            }


def feather_to_csv(pop_path: Path, votes_path: Path, grouper_candidat: str, dest: Path):
    import pandas as pd

    pop = pd.read_feather(pop_path).set_index("code")
    votes = (
        pd.read_feather(votes_path)
        .groupby(["code", grouper_candidat])["voix"]
        .sum()
        .unstack()
        .fillna(0, downcast="infer")
    )

    res = pd.concat([pop, votes], axis=1)
    res.to_csv(dest)
