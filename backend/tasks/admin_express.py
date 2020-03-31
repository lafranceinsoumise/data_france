import csv
import json

import fiona

__all__ = ["task_extraire_polygones_communes"]

from backend import PREPARE_DIR

COMMUNES_GEOMETRY = PREPARE_DIR / "ign/admin-express/version-cog/communes-geometrie.csv"


def task_extraire_polygones_communes():
    shp_file = (
        PREPARE_DIR
        / "ign/admin-express/version-cog/ADMIN-EXPRESS-COG_2-0__SHP__FRA_2019-09-24/ADMIN-EXPRESS-COG/1_DONNEES_LIVRAISON_2019-09-24/ADE-COG_2-0_SHP_WGS84_FR/COMMUNE_CARTO.shp"
    )
    temp_file = COMMUNES_GEOMETRY.with_suffix(".temp")

    return {
        "file_dep": [shp_file],
        "task_dep": ["decompresser"],
        "targets": [COMMUNES_GEOMETRY],
        "actions": [
            (extraires_polygones_communes, [shp_file, temp_file],),
            f"( \
                    head -n 1 {temp_file}; \
                    tail -n +2  {temp_file} | grep ^COM, | sort -k1,2 -t, ;\
                    tail -n +2  {temp_file} | grep -v ^COM, | sort -k1,2 -t, \
              ) > {COMMUNES_GEOMETRY}",
            f"rm {temp_file}",
        ],
    }


def to_multipolygon(geometry):
    if geometry["type"] == "Polygon":
        res = [geometry["coordinates"]]
    else:
        res = geometry["coordinates"]

    return json.dumps(res, separators=(",", ":"))


def extraires_polygones_communes(shp_path, csv_path):
    with fiona.open(shp_path) as shp, open(csv_path, mode="w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["type", "code", "geometry"])
        for com in iter(shp):
            w.writerow(
                [
                    com["properties"]["TYPE"].strip(),
                    com["properties"]["INSEE_COM"].strip(),
                    to_multipolygon(com["geometry"]),
                ]
            )
