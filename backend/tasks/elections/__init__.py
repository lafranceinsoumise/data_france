from pathlib import Path

from doit.tools import create_folder

from sources import SOURCE_DIR, PREPARE_DIR, SOURCES
from .scrutins_2014 import clean_results as clean_results_2014
from .scrutins_2017_2020 import clean_results as clean_results_post_2017

__all__ = ["task_preparer", "task_corriger_municipales_2020_tour_1"]


sans_deuxieme_tour = {"europeennes"}

RESULTATS_DIR = PREPARE_DIR / "interieur" / "resultats_electoraux"


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


def task_preparer():
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

        if int(annee) >= 2017:
            func = clean_results_post_2017
        else:
            func = clean_results_2014

        yield {
            "name": source.path,
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
