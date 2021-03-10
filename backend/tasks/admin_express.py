import csv

import fiona

__all__ = ["task_extraire_polygones_communes"]

from shapely.geometry import shape, Polygon, MultiPolygon

from soures import PREPARE_DIR

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
