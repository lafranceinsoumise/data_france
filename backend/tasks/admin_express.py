import os
import json
import csv
import subprocess
import tempfile
from functools import reduce
from itertools import product, groupby
from operator import itemgetter
from pathlib import Path, PurePath
from tempfile import TemporaryDirectory


import fiona
import pandas as pd
from doit.tools import create_folder
from shapely.geometry import shape, MultiPolygon
from shapely import wkb
from py7zr import SevenZipFile

from sources import PREPARE_DIR, SOURCE_DIR, SOURCES
from .cog import COMMUNE_TYPE_ORDERING
from contextlib import ExitStack

__all__ = [
    "task_decompresser_admin_express",
    "task_extraire_geometries_communes",
    "task_extraire_geometries_cantons",
    "task_simplifier_geometries",
    "task_trier_et_normaliser_geometries",
]

COMMUNES_GEOMETRY = PREPARE_DIR / "ign" / "admin-express" / "communes_geometries.csv"
CANTONS_GEOMETRY = PREPARE_DIR / "ign" / "admin-express" / "cantons_geometries.csv"


GEOMETRIES_COMMUNES = {
    "COMMUNE": ("COM", "INSEE_COM"),
    "COMMUNE_ASSOCIEE_OU_DELEGUEE": (None, "INSEE_CAD"),
    "ARRONDISSEMENT_MUNICIPAL": ("ARM", "INSEE_ARM"),
}

EXTRAIRE_GEOMETRIES = [*GEOMETRIES_COMMUNES, "CANTON"]


EXTS = [".shp", ".cpg", ".dbf", ".prj", ".shx"]

METROPOLE_QUANTIZATION = "1e5"
OUTREMER_QUANTIZATION = "1e6"
MIN_SPHERICAL_TRIANGLE_AREA = "1e-9"

# Il faut augmenter significativement la mémoire disponible pour pouvoir faire
# tourner topojson
NODE_OPTIONS = "--max_old_space_size=6000"


def task_decompresser_admin_express():
    """Décompression des fichiers nécessaires depuis l'archive admin-express

    Permet d'éviter d'avoir à l'ensemble de l'archive.
    """
    source = SOURCES.ign["admin-express"]["version-cog"]
    archive = SOURCE_DIR / source.filename
    dest_dir = PREPARE_DIR / source.path
    targets = [
        (dest_dir / g).with_suffix(s) for g, s in product(EXTRAIRE_GEOMETRIES, EXTS)
    ]

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

    shp_config = {shp_dir / f"{g}.shp": c for g, c in GEOMETRIES_COMMUNES.items()}
    sources = [shp_dir / f"{g}{e}" for g, e in product(GEOMETRIES_COMMUNES, EXTS)]
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


def task_extraire_geometries_cantons():
    """Extrait les polygones correspondant aux cantons

    On traite séparément la métropole et l'outremer, pour faciliter la
    quantification : il faut donc séparer les cantons dans deux fichiers.
    """

    shp_dir = PREPARE_DIR / SOURCES.ign["admin-express"]["version-cog"].path
    out_dir = PREPARE_DIR / "ign" / "admin-express"

    sources = [shp_dir / f"CANTON{e}" for e in EXTS]
    metropole = out_dir / "cantons_metropole.ndjson"
    outremer = out_dir / "cantons_outremer.ndjson"

    return {
        "file_dep": sources,
        "targets": [metropole, outremer],
        "actions": [
            (extraire_geometries_cantons, (shp_dir / "CANTON.shp", metropole, outremer))
        ],
    }


def task_simplifier_geometries():
    ae_dir = PREPARE_DIR / "ign" / "admin-express"
    yield {
        "name": "metropole",
        "file_dep": [
            ae_dir / "communes_metropole.ndjson",
            ae_dir / "cantons_metropole.ndjson",
        ],
        "targets": [ae_dir / "topologie_metropole.csv"],
        "actions": [
            (
                simplifier_geometries,
                (
                    ae_dir / "communes_metropole.ndjson",
                    ae_dir / "cantons_metropole.ndjson",
                    ae_dir / "topologie_metropole.csv",
                    METROPOLE_QUANTIZATION,
                ),
            )
        ],
    }

    yield {
        "name": "outremer",
        "file_dep": [
            ae_dir / "communes_outremer.ndjson",
            ae_dir / "cantons_outremer.ndjson",
        ],
        "targets": [ae_dir / "topologie_outremer.csv"],
        "actions": [
            (
                simplifier_geometries,
                (
                    ae_dir / "communes_outremer.ndjson",
                    ae_dir / "cantons_outremer.ndjson",
                    ae_dir / "topologie_outremer.csv",
                    OUTREMER_QUANTIZATION,
                ),
            )
        ],
    }


def task_trier_et_normaliser_geometries():
    ae_dir = PREPARE_DIR / "ign" / "admin-express"
    sources = [
        ae_dir / "topologie_metropole.csv",
        ae_dir / "topologie_outremer.csv",
    ]

    yield {
        "name": "communes",
        "file_dep": sources,
        "targets": [COMMUNES_GEOMETRY],
        "actions": [
            (
                trier_et_normaliser_geometries,
                (),
                {
                    "obj": "communes",
                    "cle": cle_tri,
                    "inpaths": sources,
                    "outpath": COMMUNES_GEOMETRY,
                },
            )
        ],
    }

    yield {
        "name": "cantons",
        "file_dep": sources,
        "targets": [CANTONS_GEOMETRY],
        "actions": [
            (
                trier_et_normaliser_geometries,
                (),
                {
                    "obj": "cantons",
                    "cle": itemgetter("code"),
                    "inpaths": sources,
                    "outpath": CANTONS_GEOMETRY,
                },
            )
        ],
    }


def decompresser_admin_express(archive, dest_dir):
    with SevenZipFile(archive) as archive:
        all_names = [PurePath(f) for f in archive.getnames()]
        extract = [f for f in all_names if f.stem in EXTRAIRE_GEOMETRIES]
        with TemporaryDirectory() as d:
            archive.extract(d, targets=[str(f) for f in extract])
            for f in extract:
                (Path(d) / f).rename(dest_dir / f.name)


def extraire_geometries_communes(shp_config, dest_metropole, dest_outremer):
    with open(dest_metropole, mode="w") as fm, open(dest_outremer, mode="w") as fo:
        for shp_path, conf in shp_config.items():
            with fiona.open(shp_path) as shp:
                for com in shp:
                    props = {
                        "type": conf[0] or com["properties"]["NATURE"].strip(),
                        "code": com["properties"][conf[1]].strip(),
                    }
                    feature = {
                        "type": "Feature",
                        "geometry": com["geometry"].__geo_interface__,
                        "properties": props,
                    }
                    if props["code"][:2] in ["97", "98"]:
                        f = fo
                    else:
                        f = fm
                    json.dump(feature, f, separators=(",", ":"))
                    f.write("\n")


def extraire_geometries_cantons(shp_path, dest_metropole, dest_outremer):
    with dest_metropole.open("w") as fm, dest_outremer.open("w") as fo, fiona.open(
        shp_path
    ) as shp:
        for canton in shp:
            p = canton["properties"]
            props = {"code": f'{p["INSEE_DEP"]}{p["INSEE_CAN"]}'}
            feature = {
                "geometry": canton.__geo_interface__["geometry"],
                "properties": props,
                "type": "Feature",
            }

            if len(p["INSEE_DEP"]) == 3:
                f = fo
            else:
                f = fm
            json.dump(feature, f, separators=(",", ":"))
            f.write("\n")


def simplifier_geometries(
    geometries_communes, geometries_cantons, dest_topologie, quantization
):
    env = {"NODE_OPTIONS": NODE_OPTIONS, "PATH": os.environ["PATH"]}

    # préquantifier à ce stade est malheureusement obligatoire pour pouvoir
    # faire tourner cette étape sans sortir un fichier JSON trop gros pour
    # la limite (dure) de taille de chaîne de caractères de V8.
    geo2topo = subprocess.Popen(
        [
            "geo2topo",
            "-n",
            f"communes={geometries_communes}",
            f"cantons={geometries_cantons}",
            "-q",
            quantization,
        ],
        stdout=subprocess.PIPE,
        env=env,
    )

    with dest_topologie.open("w") as topologie_file:
        toposimplify = subprocess.Popen(
            ["toposimplify", "-s", MIN_SPHERICAL_TRIANGLE_AREA],
            stdin=geo2topo.stdout,
            stdout=topologie_file,
            env=env,
        )
    # pour que geo2topo puisse reçoivoir SIGPIPE si toposimplify quitte
    geo2topo.stdout.close()

    geo2topo.wait()
    toposimplify.wait()


def depuis_topologie(topologie, obj):
    env = {"NODE_OPTIONS": NODE_OPTIONS, "PATH": os.environ["PATH"]}

    # l'option -n permet de sortir un objet JSON par polygone, ce qui évite de
    # faire planter node en essayant de lui faire sortir un unique json
    with open(topologie) as f:
        topo2geo = subprocess.Popen(
            ["topo2geo", "-n", f"{obj}=-"],
            stdin=f,
            stdout=subprocess.PIPE,
            env=env,
        )

    for line in iter(topo2geo.stdout.readline, b""):
        yield json.loads(line.decode())

    topo2geo.wait()


def cle_tri(l):
    return (COMMUNE_TYPE_ORDERING.index(l["type"]), l["code"])


def nettoyer_geometrie(geom, cle):
    s = shape(geom["geometry"])
    if not s.is_valid:
        # semble généralement corriger les géométries invalides Il semble s'agir
        # généralement de géometries avec un segment d'aire nulle (i.e. un bout
        # de polygone qui s'étend depuis un des coins avant de revenir par le
        # même chemin). Ces géométries sont sans doute causées par la simplification
        # de la topologie.
        s = s.buffer(0)

    return {
        "properties": geom["properties"],
        "cle": cle(geom["properties"]),
        "shape": s,
    }


def fusionner_geometries(g1, g2):
    return {
        "properties": {**g2["properties"], **g1["properties"]},
        "shape": g1["shape"].union(g2["shape"]),
    }


def serialiser_geometrie(g):
    """Prépare la géométrie pour sérialisation en csv

    Aplatit l'objet properties et sérialise la géométrie shapely en WKB (hex)
    """
    s = g["shape"]
    if not isinstance(s, MultiPolygon):
        s = MultiPolygon([s])

    return {**g["properties"], "geometry": s.wkb_hex}


def trier_et_normaliser_geometries(obj, cle, inpaths, outpath):
    geoms = []

    for p in inpaths:
        geoms.extend(nettoyer_geometrie(g, cle) for g in depuis_topologie(p, obj))

    # trier en utilisant la clé précalculée par `nettoyer_geometrie`
    geoms.sort(key=itemgetter("cle"))

    # fusionner géométries qui partagent la même clé et sérialiser
    data = [
        serialiser_geometrie(reduce(fusionner_geometries, gs))
        for _, gs in groupby(geoms, key=itemgetter("cle"))
    ]

    with outpath.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=data[0].keys())
        w.writeheader()
        w.writerows(data)
