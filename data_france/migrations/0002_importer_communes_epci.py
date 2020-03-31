import csv
import json
import lzma
from itertools import islice
from pathlib import Path

from django.contrib.gis.geos import MultiPolygon, Polygon
from django.db import migrations


DATA_DIR = Path(__file__).parent.parent / "data"


def importer_epci(apps, schema):
    EPCI = apps.get_model("data_france", "EPCI")

    with lzma.open(DATA_DIR / "epci.csv.lzma", "rt", newline="") as f:
        r = enumerate(csv.DictReader(f))
        while True:
            epcis = list(islice(r, 500))
            if not epcis:
                break
            EPCI.objects.bulk_create([EPCI(id=i, **epci) for i, epci in epcis])


def supprimer_epci(apps, schema):
    EPCI = apps.get_model("data_france", "EPCI")

    EPCI.objects.all().delete()


def get_geometry(c):
    if not c["geometry"]:
        return None
    return MultiPolygon(*(Polygon(*ring) for ring in json.loads(c["geometry"])))


def importer_communes(apps, schema):
    EPCI = apps.get_model("data_france", "EPCI")
    Commune = apps.get_model("data_france", "Commune")

    epci_ids = {e["code"]: e["id"] for e in EPCI.objects.values("code", "id")}
    communes_ids = {("COM", ""): None}

    with lzma.open(DATA_DIR / "communes.csv.lzma", "rt", newline="") as f:
        r = enumerate(csv.DictReader(f))
        while True:
            communes = list(islice(r, 500))
            if not communes:
                break
            Commune.objects.bulk_create(
                [
                    Commune(
                        id=communes_ids.setdefault((c["type"], c["code"]), i),
                        epci_id=epci_ids[c["epci"]] if c["epci"] else None,
                        commune_parent_id=communes_ids[("COM", c["commune_parent"])],
                        population_municipale=c["population_municipale"] or None,
                        population_cap=c["population_cap"] or None,
                        geometry=get_geometry(c),
                        **{
                            k: v
                            for k, v in c.items()
                            if k
                            not in [
                                "epci",
                                "commune_parent",
                                "population_municipale",
                                "population_cap",
                                "geometry",
                            ]
                        },
                    )
                    for i, c in communes
                ]
            )


def supprimer_communes(apps, schema):
    Commune = apps.get_model("data_france", "Commune")

    Commune.objects.all().delete()


class Migration(migrations.Migration):
    dependencies = [("data_france", "0001_initial")]

    operations = [
        migrations.RunPython(code=importer_epci, reverse_code=supprimer_epci),
        migrations.RunPython(code=importer_communes, reverse_code=supprimer_communes),
    ]
