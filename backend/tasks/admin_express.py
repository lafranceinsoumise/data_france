import csv
import json

import fiona

__all__ = ["task_communes_vers_csv"]

from backend import PREPARE_DIR, BASE_DIR

COMMUNES_GEOMETRY = BASE_DIR / "data_france/data/communes-geometrie.csv"


def task_communes_vers_csv():
    shp_file = (
        PREPARE_DIR
        / "ign/admin-express/version-cog/ADMIN-EXPRESS-COG_2-0__SHP__FRA_2019-09-24/ADMIN-EXPRESS-COG/1_DONNEES_LIVRAISON_2019-09-24/ADE-COG_2-0_SHP_WGS84_FR/COMMUNE_CARTO.shp"
    )
    return {
        "file_dep": [shp_file],
        "task_dep": ["decompresser"],
        "targets": [COMMUNES_GEOMETRY],
        "actions": [(communes_to_csv, [shp_file, COMMUNES_GEOMETRY])],
    }


def to_multipolygon(geometry):
    if geometry["type"] == "Polygon":
        geometry = {"type": "MultiPolygon", "coordinates": [geometry["coordinates"]]}

    return json.dumps(geometry, separators=(",", ":"))


def communes_to_csv(shp_path, csv_path):
    with fiona.open(shp_path) as shp, open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["type", "code", "geometrie"])
        for com in iter(shp):
            w.writerow(
                [
                    com["properties"]["TYPE"],
                    com["properties"]["INSEE_COM"],
                    to_multipolygon(com["geometry"]),
                ]
            )
