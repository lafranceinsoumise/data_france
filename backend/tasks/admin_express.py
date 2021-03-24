import csv
import tempfile
from pathlib import Path

import fiona
import pandas as pd
from shapely.geometry import shape, Polygon, MultiPolygon

from sources import PREPARE_DIR
from .cog import COMMUNE_TYPE_ORDERING
from contextlib import ExitStack
import heapq

__all__ = ["task_extraire_polygones_communes"]

COMMUNES_GEOMETRY = PREPARE_DIR / "ign/admin-express/version-cog/communes-geometrie.csv"


def task_extraire_polygones_communes():
    shp_dir = (
        PREPARE_DIR
        / "ign"
        / "admin-express"
        / "version-cog"
        / "ADMIN-EXPRESS-COG_2-1__SHP__FRA_2020-11-20"
        / "ADMIN-EXPRESS-COG"
        / "1_DONNEES_LIVRAISON_2020-11-20"
        / "ADE-COG_2-1_SHP_WGS84G_FRA"
    )

    # attention, l'ordre des fichiers est important, les COM doivent être avant les autres types
    shp_files = [
        shp_dir / "COMMUNE_CARTO.shp",
        shp_dir / "ARRONDISSEMENT_MUNICIPAL.shp",
    ]

    return {
        "file_dep": shp_files,
        "task_dep": ["decompresser"],
        "targets": [COMMUNES_GEOMETRY],
        "actions": [
            (
                extraires_polygones_communes,
                [shp_files, COMMUNES_GEOMETRY],
            ),
            (trier_polygones_communes, [COMMUNES_GEOMETRY]),
        ],
    }


def to_multipolygon(geometry):
    s = shape(geometry)
    if not s.is_valid:
        # semble généralement corriger les géométries invalides
        # je l'ai vérifié à la main pour les 4 communes problématiques
        s = s.buffer(0)
    if isinstance(s, Polygon):
        s = MultiPolygon([s])
    return s.wkb_hex


def extraires_polygones_communes(shp_files, csv_path):
    with open(csv_path, mode="w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["type", "code", "geometry"])

        for shp_path in shp_files:
            with fiona.open(shp_path) as shp:
                for com in iter(shp):
                    w.writerow(
                        [
                            com["properties"]["TYPE"].strip(),
                            com["properties"]["INSEE_COM"].strip(),
                            to_multipolygon(com["geometry"]),
                        ]
                    )


def trier_polygones_communes(csv_path):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_files = []
        with ExitStack() as stack:
            for i, df in enumerate(
                pd.read_csv(
                    csv_path, dtype={"code": "str", "geometry": "str"}, chunksize=1000
                )
            ):
                path = Path(tmpdir) / f"{i}.csv"
                df.sort_values(
                    by=["type", "code"],
                    key=lambda s: s.map(COMMUNE_TYPE_ORDERING.index)
                    if s.name == "type"
                    else s,
                ).to_csv(path, index=False)
                tmp_files.append(stack.enter_context(path.open("r")))

            chunks = i + 1

            with csv_path.open("w") as fd:
                w = csv.DictWriter(fd, fieldnames=df.columns)
                w.writeheader()
                w.writerows(
                    heapq.merge(
                        *(csv.DictReader(f) for f in tmp_files),
                        key=lambda line: (
                            COMMUNE_TYPE_ORDERING.index(line["type"]),
                            line["code"],
                        ),
                    )
                )
