from backend import SOURCE_DIR, PREPARE_DIR
from sources import iterate_sources
from utils import ensure_dir_exists

from .scrutins_2017_2019 import clean_results as clean_results_post_2017
from .scrutins_2014 import clean_results as clean_results_2014

__all__ = ["task_preparer"]


sans_deuxieme_tour = {"europeennes"}


def task_preparer():
    for source in iterate_sources():
        if source.path.parts[:2] == ["interieur", "resultats_electoraux"]:
            election, annee = source.path.parts[2:4]
            tour = (
                int(source.path.parts[4][-1]) if len(source.path.parts) == 5 else None
            )

            src = SOURCE_DIR / source.filename
            if source.to_csv:
                src = src.with_suffix(".csv")

            if tour:
                base_filenames = [PREPARE_DIR / f"{annee}-{election}-{tour}"]
            elif election in sans_deuxieme_tour:
                base_filenames = [PREPARE_DIR / f"{annee}-{election}"]
            else:
                base_filenames = [
                    PREPARE_DIR
                    / "interieur"
                    / "resultats_electoraux"
                    / f"{annee}-{election}-{tour}"
                    for tour in [1, 2]
                ]

            targets = [
                f.with_name(f.name + ext)
                for f in base_filenames
                for ext in ["-pop.feather", "-votes.feather"]
            ]

            if int(annee) <= 2017:
                func = clean_results_post_2017
            else:
                func = clean_results_2014

            yield {
                "name": source.path,
                "targets": targets,
                "file_dep": [src],
                "actions": [
                    *[ensure_dir_exists(t) for t in base_filenames],
                    (
                        func,
                        [],
                        {
                            "src": src,
                            "base_filenames": base_filenames,
                            "delimiter": "," if source.to_csv else ";",
                        },
                    ),
                ],
            }
