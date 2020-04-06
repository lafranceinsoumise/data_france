import contextlib
import csv
import lzma
from pathlib import Path

from doit.tools import create_folder

from backend import BASE_DIR
from tasks.admin_express import COMMUNES_GEOMETRY
from tasks.cog import COMMUNES_CSV, EPCI_CSV

REFERENCES_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_france" / "data"
FINAL_COMMUNES = DATA_DIR / "communes.csv.lzma"
FINAL_EPCI = DATA_DIR / "epci.csv.lzma"

NULL = r"\N"

__all__ = ["task_generer_fichier_communes", "task_generer_fichier_epci"]


ORDERING = ["COM", "ARM", "COMA", "COMD", None]


def _key(t):
    return (ORDERING.index(t["type"]), t["code"])


@contextlib.contextmanager
def id_from_file(path, read_only=False):
    reference = {}

    with open(path, "r") as f:
        r = csv.DictReader(f)
        columns = [c for c in r.fieldnames if c != "id"]

        for d in r:
            id = d.pop("id")
            reference[tuple(d[c] for c in columns)] = id

    last_id = max(reference.values()) if reference else -1

    def get_id(**kwargs):
        nonlocal last_id
        key = tuple(kwargs[c] for c in columns)
        if key in reference:
            return reference[key]
        else:
            if read_only:
                raise KeyError(f"ID inconnue pour {kwargs!r} dans {path}")
            last_id += 1
            return reference.setdefault(key, last_id)

    yield get_id

    with open(path, "w") as f:
        w = csv.writer(f,)
        w.writerow(columns + ["id"])
        w.writerows([*t, id] for t, id in reference.items())


def task_generer_fichier_communes():
    return {
        "file_dep": [COMMUNES_CSV, COMMUNES_GEOMETRY, EPCI_CSV],
        "targets": [FINAL_COMMUNES],
        "actions": [
            (create_folder, [DATA_DIR]),
            (
                generer_fichier_communes,
                [COMMUNES_CSV, COMMUNES_GEOMETRY, EPCI_CSV, FINAL_COMMUNES],
            ),
        ],
    }


def task_generer_fichier_epci():
    return {
        "file_dep": [EPCI_CSV],
        "targets": [FINAL_EPCI],
        "actions": [
            (create_folder, [DATA_DIR]),
            (generer_fichier_epci, [EPCI_CSV, FINAL_EPCI]),
        ],
    }


def generer_fichier_epci(path, lzma_path):
    with open(path, "r") as f, lzma.open(lzma_path, "wt") as l, id_from_file(
        REFERENCES_DIR / "epci.csv"
    ) as get_id:

        r = csv.DictReader(f)
        w = csv.DictWriter(l, fieldnames=["id", *r.fieldnames])
        w.writeheader()
        w.writerows({"id": get_id(code=epci["code"]), **epci} for epci in r)


COMMUNES_FIELDS = [
    "id",
    "code",
    "code_departement",
    "type",
    "nom",
    "type_nom",
    "population_municipale",
    "population_cap",
    "commune_parent_id",
    "epci_id",
    "geometry",
]


def generer_fichier_communes(communes, communes_geo, epci, dest):
    csv.field_size_limit(2 * 131072)  # double default limit

    with open(communes, "r", newline="") as fc, open(
        communes_geo, "r", newline=""
    ) as fg, lzma.open(dest, "wt") as fl, id_from_file(
        REFERENCES_DIR / "communes.csv"
    ) as commune_id, id_from_file(
        REFERENCES_DIR / "epci.csv"
    ) as epci_id:
        rc = csv.DictReader(fc)
        rg = csv.DictReader(fg)

        w = csv.DictWriter(fl, fieldnames=COMMUNES_FIELDS)
        w.writeheader()

        geometry = next(rg)

        for commune in rc:
            k = _key(commune)
            while k > _key(geometry):
                try:
                    geometry = next(rg)
                except StopIteration:
                    geometry = {"type": None, "code": ""}

            if k == _key(geometry):
                commune["geometry"] = geometry["geometry"]
            else:
                commune["geometry"] = NULL

            w.writerow(
                {
                    "id": commune_id(type=commune["type"], code=commune["code"]),
                    "code": commune["code"],
                    "code_departement": commune["code_departement"],
                    "type": commune["type"],
                    "nom": commune["nom"],
                    "type_nom": commune["type_nom"],
                    "population_municipale": commune["population_municipale"] or NULL,
                    "population_cap": commune["population_cap"] or NULL,
                    "commune_parent_id": commune_id(
                        type="COM", code=commune["commune_parent"]
                    )
                    if commune["commune_parent"]
                    else NULL,
                    "epci_id": epci_id(code=commune["epci"])
                    if commune["epci"]
                    else NULL,
                    "geometry": commune["geometry"],
                }
            )
