import csv
import lzma

from doit.tools import create_folder

from backend import BASE_DIR
from tasks.admin_express import COMMUNES_GEOMETRY
from tasks.cog import COMMUNES_CSV, EPCI_CSV

DATA_DIR = BASE_DIR / "data_france" / "data"
FINAL_COMMUNES = DATA_DIR / "communes.csv.lzma"
FINAL_EPCI = DATA_DIR / "epci.csv.lzma"

CHUNK_SIZE = 10_000

__all__ = ["task_generer_fichier_communes", "task_generer_fichier_epci"]


ORDERING = ["COM", "ARM", "COMA", "COMD", None]


def _key(t):
    return (ORDERING.index(t["type"]), t["code"])


def task_generer_fichier_communes():
    return {
        "file_dep": [COMMUNES_CSV, COMMUNES_GEOMETRY],
        "targets": [FINAL_COMMUNES],
        "actions": [
            (create_folder, [DATA_DIR]),
            (
                generer_fichier_communes,
                [COMMUNES_CSV, COMMUNES_GEOMETRY, FINAL_COMMUNES],
            ),
        ],
    }


def task_generer_fichier_epci():
    return {
        "file_dep": [EPCI_CSV],
        "targets": [FINAL_EPCI],
        "actions": [
            (create_folder, [DATA_DIR]),
            (compresser_lzma, [EPCI_CSV, FINAL_EPCI]),
        ],
    }


def compresser_lzma(path, lzma_path):
    with open(path, "rb") as f, lzma.open(lzma_path, "wb") as l:
        chunk = f.read(CHUNK_SIZE)
        while chunk:
            l.write(chunk)
            chunk = f.read(CHUNK_SIZE)


def generer_fichier_communes(communes, communes_geo, dest):
    with open(communes, "r", newline="") as fc, open(
        communes_geo, "r", newline=""
    ) as fg, lzma.open(dest, "wt") as fl:
        rc = csv.DictReader(fc)
        rg = csv.DictReader(fg)

        w = csv.DictWriter(fl, fieldnames=rc.fieldnames + ["geometry"])
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
                commune["geometry"] = ""

            w.writerow(commune)
