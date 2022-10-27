import csv
import json
import re

from doit import create_after
from glom import glom, Iter, Match, SKIP, Not, S, T

from sources import PREPARE_DIR, SOURCES, SOURCE_DIR
from zipfile import ZipFile
from functools import partial
from doit.tools import create_folder


__all__ = ["task_extraire_groupes_partis", "task_extraire_deputes"]


ASSEMBLEE_NATIONALE_DIR = PREPARE_DIR / "assemblee_nationale"


ORGANE_RE = re.compile(r"organe/PO\d+\.json$")
ACTEUR_RE = re.compile(r"acteur/PA\d+\.json$")


def parser_deputes(path, archive):
    with archive.open(path) as f:
        content = json.load(f)

    depute = content["acteur"]
    mandats = depute["mandats"]["mandat"]

    if not isinstance(mandats, list):
        mandats = [mandats]

    try:
        mandat_depute = next(
            o
            for o in mandats
            if o["@xsi:type"] == "MandatParlementaire_type"
            and o["infosQualite"]["codeQualite"] == "membre"
        )
    except StopIteration:
        return SKIP

    return {**depute, "mandatDepute": mandat_depute}


def numero_circonscription(lieu):
    dep = (
        lieu["numDepartement"].lstrip("0").rjust(2, "0")
        if lieu["numDepartement"]
        else "XX"
    )
    num = lieu["numCirco"].rjust(2, "0") if lieu["numCirco"] else "XX"
    return f"{dep}-{num}"


spec_membre = {
    "code_depute": S["code_depute"],
    "date_debut": "dateDebut",
    "date_fin": "dateFin",
    "code": "organes.organeRef",
}

spec_depute = {
    "code": "uid.#text",
    "circonscription": (
        "mandatDepute.election.lieu",
        numero_circonscription,
    ),
    "nom": "etatCivil.ident.nom",
    "prenom": "etatCivil.ident.prenom",
    "sexe": ("etatCivil.ident.civ", {"M.": "M", "Mme": "F"}.get),
    "date_naissance": "etatCivil.infoNaissance.dateNais",
    "date_debut_mandat": "mandatDepute.dateDebut",
    "date_fin_mandat": "mandatDepute.dateFin",
    "legislature": "mandatDepute.legislature",
    "emails": (
        "adresses.adresse",
        [
            Match(
                {"@xsi:type": "AdresseMail_Type", object: object},
                default=SKIP,
            ),
        ],
        ["valElec"],
        "/".join,
    ),
    "groupes": (
        S(code_depute=T["uid"]["#text"]),
        "mandats.mandat",
        [
            Match(
                {
                    "typeOrgane": "GP",
                    "infosQualite": Not(
                        {"codeQualite": "Député non-inscrit", object: object}
                    ),
                    object: object,
                },
                default=SKIP,
            ),
        ],
        [
            {
                **spec_membre,
                "relation": (
                    "infosQualite.codeQualite",
                    {"Membre": "M", "Président": "P", "Membre apparenté": "A"}.get,
                ),
            }
        ],
    ),
    "partis": (
        S(code_depute=T["uid"]["#text"]),
        "mandats.mandat",
        [Match({"typeOrgane": "PARPOL", object: object}, default=SKIP)],
        [spec_membre],
    ),
}


spec_organes = {
    "GP": {
        "code": "uid",
        "nom": "libelle",
        "sigle": "libelleAbrege",
    },
    "PARPOL": {"code": "uid", "nom": "libelle", "sigle": "libelleAbrev"},
}


def task_extraire_groupes_partis():
    archive = SOURCE_DIR / SOURCES.assemblee_nationale.deputes.filename
    organes = list(
        (ASSEMBLEE_NATIONALE_DIR / "deputes" / "json" / "organe").glob("PO*.json")
    )
    groupes = ASSEMBLEE_NATIONALE_DIR / "groupes.csv"
    partis = ASSEMBLEE_NATIONALE_DIR / "partis.csv"

    return {
        "file_dep": [archive],
        "targets": [groupes, partis],
        "actions": [
            (create_folder, (ASSEMBLEE_NATIONALE_DIR,)),
            (
                extraire_groupes_partis,
                (),
                {"archive": archive, "groupes": groupes, "partis": partis},
            ),
        ],
    }


def task_extraire_deputes():
    archive = SOURCE_DIR / SOURCES.assemblee_nationale.deputes.filename
    deputes = ASSEMBLEE_NATIONALE_DIR / "deputes.csv"
    deputes_partis = ASSEMBLEE_NATIONALE_DIR / "deputes_partis.csv"
    deputes_groupes = ASSEMBLEE_NATIONALE_DIR / "deputes_groupes.csv"

    return {
        "file_dep": [archive],
        "targets": [deputes, deputes_partis, deputes_groupes],
        "actions": [
            (create_folder, (ASSEMBLEE_NATIONALE_DIR,)),
            (
                extraires_deputes,
                (),
                {
                    "archive": archive,
                    "deputes": deputes,
                    "deputes_partis": deputes_partis,
                    "deputes_groupes": deputes_groupes,
                },
            ),
        ],
    }


def extraire_groupes_partis(archive, groupes, partis):
    with ZipFile(archive) as arc, groupes.open("w") as f_groupe, partis.open(
        "w"
    ) as f_partis:
        fieldnames = ["code", "nom"]
        writers = {
            "GP": csv.DictWriter(f_groupe, fieldnames=spec_organes["GP"]),
            "PARPOL": csv.DictWriter(f_partis, fieldnames=spec_organes["PARPOL"]),
        }
        for w in writers.values():
            w.writeheader()

        organes = (g for g in arc.namelist() if ORGANE_RE.search(g))

        for path in organes:
            with arc.open(path) as fd:
                content = json.load(fd)["organe"]

            if (typ := content["codeType"]) in writers:
                writers[typ].writerow(glom(content, spec_organes[typ]))


def extraires_deputes(archive, deputes, deputes_partis, deputes_groupes):
    with ZipFile(archive) as arc, deputes.open("w") as f_deputes, deputes_partis.open(
        "w"
    ) as f_partis, deputes_groupes.open("w") as f_groupes:
        w = csv.DictWriter(
            f_deputes,
            fieldnames=[f for f in spec_depute if f not in ["groupes", "partis"]],
        )
        wp = csv.DictWriter(f_partis, fieldnames=spec_membre)
        wg = csv.DictWriter(f_groupes, fieldnames=[*spec_membre, "relation"])

        w.writeheader()
        wp.writeheader()
        wg.writeheader()

        acteurs = (a for a in arc.namelist() if ACTEUR_RE.search(a))

        for d in glom(
            acteurs, Iter(partial(parser_deputes, archive=arc)).map(spec_depute)
        ):
            wp.writerows(d.pop("partis"))
            wg.writerows(d.pop("groupes"))
            w.writerow(d)
