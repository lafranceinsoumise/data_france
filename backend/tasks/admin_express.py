import os
import json
import csv
import subprocess
import tempfile
from itertools import product
from pathlib import Path, PurePath

import fiona
import pandas as pd
from doit.tools import create_folder
from libarchive.public import file_reader as archive_reader
from shapely.geometry import shape, MultiPolygon
from shapely import wkb

from sources import PREPARE_DIR, SOURCE_DIR, SOURCES
from .cog import COMMUNE_TYPE_ORDERING
from contextlib import ExitStack
import heapq

__all__ = [
    "task_decompresser_admin_express",
    "task_extraire_geometries_communes",
    "task_simplifier_geometries_communes",
    "task_trier_et_normaliser_geometries_communes",
]

COMMUNES_GEOMETRY = PREPARE_DIR / "ign" / "admin-express" / "communes_geometries.csv"

GEOMETRIES = {
    "COMMUNE": ("COM", "INSEE_COM"),
    "COMMUNE_ASSOCIEE_OU_DELEGUEE": (None, "INSEE_CAD"),
    "ARRONDISSEMENT_MUNICIPAL": ("ARM", "INSEE_ARM"),
}

EXTS = [".shp", ".cpg", ".dbf", ".prj", ".shx"]

METROPOLE_QUANTIZATION = "1e5"
OUTREMER_QUANTIZATION = "1e6"
MIN_SPHERICAL_TRIANGLE_AREA = "1e-9"
NODE_OPTIONS = "--max_old_space_size=4096"


def task_decompresser_admin_express():
    """Décompression des fichiers nécessaires depuis l'archive admin-express

    Permet d'éviter d'avoir à l'ensemble de l'archive.
    """
    source = SOURCES.ign["admin-express"]["version-cog"]
    archive = SOURCE_DIR / source.filename
    dest_dir = PREPARE_DIR / source.path
    targets = [(dest_dir / g).with_suffix(s) for g, s in product(GEOMETRIES, EXTS)]

    return {
        "file_dep": [archive],
        "targets": targets,
        "actions": [
            (create_folder, (dest_dir,)),
            (decompresser_admin_express, (archive, dest_dir)),
        ],
    }


def task_extraire_geometries_communes():
    """Extrait l'ensemble des polygones correspondant aux communes

    Pour pouvoir réaliser la simplification des géométries, on sépare les
    communes d'outremer dans un autre fichier : cela rendra la quantification du
    fichier de métropole beaucoup plus efficace.
    """
    shp_dir = PREPARE_DIR / SOURCES.ign["admin-express"]["version-cog"].path
    out_dir = PREPARE_DIR / "ign" / "admin-express"

    shp_config = {shp_dir / f"{g}.shp": c for g, c in GEOMETRIES.items()}
    sources = [shp_dir / f"{g}{e}" for g, e in product(GEOMETRIES, EXTS)]
    metropole = out_dir / "communes_metropole.ndjson"
    outremer = out_dir / "communes_outremer.ndjson"

    return {
        "file_dep": sources,
        "targets": [metropole, outremer],
        "actions": [
            (
                extraire_geometries_communes,
                [shp_config, metropole, outremer],
            ),
        ],
    }


def task_simplifier_geometries_communes():
    ae_dir = PREPARE_DIR / "ign" / "admin-express"
    yield {
        "name": "metropole",
        "file_dep": [ae_dir / "communes_metropole.ndjson"],
        "targets": [ae_dir / "communes_metropole.csv"],
        "actions": [
            (
                simplifier_geometries_communes,
                (
                    ae_dir / "communes_metropole.ndjson",
                    ae_dir / "communes_metropole.csv",
                    METROPOLE_QUANTIZATION,
                ),
            )
        ],
    }

    yield {
        "name": "outremer",
        "file_dep": [ae_dir / "communes_outremer.ndjson"],
        "targets": [ae_dir / "communes_outremer.csv"],
        "actions": [
            (
                simplifier_geometries_communes,
                (
                    ae_dir / "communes_outremer.ndjson",
                    ae_dir / "communes_outremer.csv",
                    OUTREMER_QUANTIZATION,
                ),
            )
        ],
    }


def task_trier_et_normaliser_geometries_communes():
    ae_dir = PREPARE_DIR / "ign" / "admin-express"
    sources = [ae_dir / "communes_metropole.csv", ae_dir / "communes_outremer.csv"]
    target = COMMUNES_GEOMETRY

    return {
        "file_dep": sources,
        "targets": [target],
        "actions": [(trier_et_normaliser_geometries_communes, (sources, target))],
    }


def decompresser_admin_express(archive, dest_dir):
    with archive_reader(str(archive)) as r:
        for entry in r:
            p = PurePath(entry.pathname)
            if p.stem in GEOMETRIES:
                dest = dest_dir / p.name

                with dest.open("wb") as f:
                    for block in entry.get_blocks():
                        f.write(block)


def extraire_geometries_communes(shp_config, dest_metropole, dest_outremer):
    with open(dest_metropole, mode="w") as fm, open(dest_outremer, mode="w") as fo:
        for shp_path, conf in shp_config.items():
            with fiona.open(shp_path) as shp:
                for com in shp:
                    com["properties"] = {
                        "type": conf[0] or com["properties"]["NATURE"].strip(),
                        "code": com["properties"][conf[1]].strip(),
                    }
                    if com["properties"]["code"][:2] in ["97", "98"]:
                        f = fo
                    else:
                        f = fm
                    json.dump(com, f, separators=(",", ":"))
                    f.write("\n")


def simplifier_geometries_communes(geometries, dest, quantization):
    env = {"NODE_OPTIONS": NODE_OPTIONS, "PATH": os.environ["PATH"]}

    with geometries.open() as f:
        geo2topo = subprocess.Popen(
            ["geo2topo", "-n", "communes=-", "-q", quantization],
            stdin=f,
            stdout=subprocess.PIPE,
            env=env,
        )
    toposimplify = subprocess.Popen(
        ["toposimplify", "-s", MIN_SPHERICAL_TRIANGLE_AREA],
        stdin=geo2topo.stdout,
        stdout=subprocess.PIPE,
        env=env,
    )
    # pour que geo2topo puisse reçoivoir SIGPIPE si toposimplify quitte
    geo2topo.stdout.close()

    topo2geo = subprocess.Popen(
        ["topo2geo", "-n", "communes=-"],
        stdin=toposimplify.stdout,
        stdout=subprocess.PIPE,
        env=env,
    )
    # pour que toposimplify reçoive SIGPIPE si topo2geo quitte
    toposimplify.stdout.close()

    with open(dest, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["type", "code", "geometry"])

        for line in iter(topo2geo.stdout.readline, b""):
            com = json.loads(line.decode())
            geom = shape(com["geometry"])
            writer.writerow(
                [com["properties"]["type"], com["properties"]["code"], geom.wkb_hex]
            )


def cle_tri(l):
    return (COMMUNE_TYPE_ORDERING.index(l["type"]), l["code"])


def normaliser_geometrie(geometry):
    s = wkb.loads(geometry, hex=True)
    if not s.is_valid:
        # semble généralement corriger les géométries invalides Il semble s'agir
        # généralement de géometries avec un segment d'aire nulle (i.e. un bout
        # de polygone qui s'étend depuis un des coins avant de revenir par le
        # même chemin). Ces géométries sont sans doute causées par la simplification
        # de la topologie.
        s = s.buffer(0)
    if not isinstance(s, MultiPolygon):
        s = MultiPolygon([s])
    return s.wkb_hex


def trier_et_normaliser_geometries_communes(inpaths, outpath):
    lines = []

    for p in inpaths:
        with p.open() as f:
            r = csv.DictReader(f)
            lines.extend(r)

    lines.sort(key=cle_tri)

    for i in range(len(lines)):
        lines[i]["geometry"] = normaliser_geometrie(lines[i]["geometry"])

    with outpath.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=lines[0].keys())
        w.writeheader()
        w.writerows(lines)
