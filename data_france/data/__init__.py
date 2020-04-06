import contextlib
import lzma
from importlib.resources import open_binary

from django.db import connections

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


def importer_epci():
    with open_binary("data_france.data", "epci.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_epci")


def importer_communes():
    with open_binary("data_france.data", "communes.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_commune")


def import_with_temp_table(csv_file, table):
    temp_table = f"{table}_temp"
    columns = [f'"{c}"' for c in csv_file.readline().strip().split(",")]

    with connections["default"].cursor() as cursor, temporary_table(
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
