import lzma
from importlib.resources import open_binary
from pathlib import Path

from django.db import migrations, connections

DATA_DIR = Path(__file__).parent.parent / "data"

COPY_SQL = (
    """COPY {table} ({columns}) FROM STDIN WITH NULL AS '\\N' CSV QUOTE AS '"';"""
)


def importer_epci(apps, schema):
    with connections["default"].cursor() as cursor, open_binary(
        "data_france.data", "epci.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
        columns = f.readline().strip()
        cursor.copy_expert(
            COPY_SQL.format(table="data_france_epci", columns=columns), f
        )


def supprimer_epci(apps, schema):
    EPCI = apps.get_model("data_france", "EPCI")

    EPCI.objects.all().delete()


def importer_communes(apps, schema):
    with connections["default"].cursor() as cursor, open_binary(
        "data_france.data", "communes.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
        columns = f.readline().strip()
        cursor.copy_expert(
            COPY_SQL.format(table="data_france_commune", columns=columns), f
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
