import csv
import lzma
from pathlib import Path

from django.contrib.gis.geos import GEOSGeometry
from django.db import migrations

DATA_DIR = Path(__file__).parent.parent / "data"


def importer_geometries(apps, schema):
    Commune = apps.get_model("data_france", "Commune")

    with lzma.open(DATA_DIR / "communes-geometrie.csv.lzma", "rt", newline="") as f:
        r = csv.DictReader(f)

        for commune in r:
            Commune.objects.filter(code=commune["code"], type=commune["type"]).update(
                geometry=GEOSGeometry(commune["geometrie"])
            )


class Migration(migrations.Migration):
    dependencies = [("data_france", "0002_importer_communes_epci")]

    operations = [
        migrations.RunPython(
            code=importer_geometries, reverse_code=migrations.RunPython.noop
        ),
    ]
