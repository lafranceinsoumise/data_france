import contextlib
import lzma
import os
from importlib.resources import open_binary
from sys import stderr

from django.db import transaction
from django.db.transaction import get_connection

COPY_SQL = (
    """COPY "{table}" ({columns}) FROM STDIN WITH NULL AS '\\N' CSV QUOTE AS '"';"""
)

CREATE_TEMP_TABLE_SQL = """
    CREATE TEMPORARY TABLE "{temp_table}" AS
    SELECT * FROM "{reference_table}" LIMIT 0;
"""

DROP_TEMPORARY_TABLE_SQL = """
    DROP TABLE IF EXISTS "{temp_table}";
"""


@contextlib.contextmanager
def temporary_table(cursor, temp_table, reference_table):
    """Context manager for creating and dropping temp tables"""
    cursor.execute(
        CREATE_TEMP_TABLE_SQL.format(
            temp_table=temp_table, reference_table=reference_table
        )
    )

    try:
        yield
    finally:
        cursor.execute(DROP_TEMPORARY_TABLE_SQL.format(temp_table=temp_table))


def import_regions(using):
    with open_binary("data_france.data", "regions.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des régions...")
        stderr.flush()
        import_with_temp_table(f, "data_france_region", using)
        stderr.write(f" OK !{os.linesep}")


def import_departements(using):
    with open_binary("data_france.data", "departements.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des départements...")
        stderr.flush()
        import_with_temp_table(f, "data_france_departement", using)
        stderr.write(f" OK !{os.linesep}")


def importer_epci(using):
    with open_binary("data_france.data", "epci.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des EPCI...")
        stderr.flush()
        import_with_temp_table(f, "data_france_epci", using)
        stderr.write(f" OK !{os.linesep}")


def importer_communes(using):
    with open_binary("data_france.data", "communes.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des communes...")
        stderr.flush()
        import_with_temp_table(f, "data_france_commune", using)
        stderr.write(f" OK !{os.linesep}")


def importer_codes_postaux(using):
    with open_binary("data_france.data", "codes_postaux.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des codes postaux...")
        stderr.flush()
        import_with_temp_table(f, "data_france_codepostal", using)
        stderr.write(f" OK !{os.linesep}")


def importer_associations_communes_codes_postaux(using):
    with open_binary(
        "data_france.data", "codes_postaux_communes.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
        stderr.write("Chargement des associations Communes/Codes postaux...")
        stderr.flush()
        columns = [f'"{c}"' for c in f.readline().strip().split(",")]
        table = "data_france_codepostal_communes"
        with get_connection(using).cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table};")
            cursor.copy_expert(
                COPY_SQL.format(table=table, columns=",".join(columns)), f
            )
        stderr.write(f" OK !{os.linesep}")


def ajouter_champs_agreges(connection):
    stderr.write("Agrégation des données par département...")
    stderr.flush()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE "data_france_departement"
            SET
                population = c.population,
                geometry = ST_Multi(c.geometry)
            FROM (
                SELECT
                    departement_id,
                    SUM(population_municipale) AS population,
                    ST_Union(geometry :: geometry) AS geometry
                FROM "data_france_commune"
                WHERE departement_id IS NOT NULL
                GROUP BY departement_id
            ) AS c
            WHERE id = c.departement_id;
            """
        )
    connection.commit()
    stderr.write(f" OK !{os.linesep}")

    stderr.write("Agrégation des données par région...")
    stderr.flush()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE "data_france_region"
            SET
                population = d.population,
                geometry = ST_Multi(d.geometry)
            FROM (
                SELECT
                    region_id,
                    SUM(population) AS population,
                    ST_Union(geometry :: geometry) AS geometry
                FROM "data_france_departement"
                GROUP BY region_id
            ) AS d
            WHERE id = d.region_id;
            """
        )
    connection.commit()
    stderr.write(f" OK !{os.linesep}")

    stderr.write("Agrégation des données par EPCI...")
    stderr.flush()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE "data_france_epci"
            SET 
                population = c.population,
                geometry = ST_Multi(c.geometry)
            FROM (
                SELECT
                    epci_id,
                    SUM(population_municipale) AS population,
                    ST_Union(geometry :: geometry) AS geometry
                FROM "data_france_commune"
                WHERE epci_id IS NOT NULL
                GROUP BY epci_id
            ) AS c
            WHERE id = c.epci_id;
            """
        )
    connection.commit()
    stderr.write(f" OK !{os.linesep}")


def import_with_temp_table(csv_file, table, using):
    temp_table = f"{table}_temp"
    columns = [f'"{c}"' for c in csv_file.readline().strip().split(",")]

    with get_connection(using).cursor() as cursor, temporary_table(
        cursor, temp_table, table
    ):
        cursor.copy_expert(
            COPY_SQL.format(table=temp_table, columns=",".join(columns)), csv_file
        )

        cursor.execute(
            f"""
                INSERT INTO {table} ({",".join(columns)})
                SELECT {",".join(f't.{c}' for c in columns)}
                FROM {temp_table} AS t
                ON CONFLICT({columns[0]}) DO UPDATE SET
                    {",".join(f'{c} = EXCLUDED.{c}' for c in columns[1:])};
        """
        )


def importer_donnees(using=None):
    auto_commit = transaction.get_autocommit(using=using)
    transaction.set_autocommit(False, using=using)
    connection = get_connection(using=using)
    try:
        importer_epci(using)
        connection.commit()

        # ces trois tables ont des foreign key croisées
        # Django crée les contraintes de clés étrangères
        # en mode "différable", ce qui permet d'importer
        # facilement ces tables en les groupant dans une
        # transaction
        import_regions(using)
        import_departements(using)
        importer_communes(using)
        connection.commit()

        importer_codes_postaux(using)
        connection.commit()

        importer_associations_communes_codes_postaux(using)
        connection.commit()

        ajouter_champs_agreges(connection)

    finally:
        transaction.rollback(using=using)
        transaction.set_autocommit(auto_commit, using)
