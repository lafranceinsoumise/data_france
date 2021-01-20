import contextlib
import csv
import lzma
import os
from importlib.resources import open_binary, open_text
from sys import stderr

from django.db import transaction
from django.db.transaction import get_connection
from psycopg2.sql import SQL, Identifier

COPY_SQL = SQL(
    """COPY {table} ({columns}) FROM STDIN WITH NULL AS '\\N' CSV QUOTE AS '"';"""
)

CREATE_TEMP_TABLE_SQL = SQL(
    """
    CREATE TEMPORARY TABLE {temp_table} AS
    SELECT {columns} FROM {reference_table} LIMIT 0;
    """
)

DROP_TEMPORARY_TABLE_SQL = SQL(
    """
    DROP TABLE IF EXISTS {temp_table};
    """
)

COPY_FROM_TEMP_TABLE = SQL(
    """
    INSERT INTO {table} ({all_columns})
    SELECT {all_columns}
    FROM {temp_table}
    ON CONFLICT({id_column}) DO UPDATE SET {setters}
    """
)

VILLES_PLM = [
    {
        "code": "13055",
        "prefixe_arm": "132",
        "secteurs": {
            1: ["01", "07"],
            2: ["02", "03"],
            3: ["04", "05"],
            4: ["06", "08"],
            5: ["09", "10"],
            6: ["11", "12"],
            7: ["13", "14"],
            8: ["15", "16"],
        },
    },
    {
        "code": "69123",
        "prefixe_arm": "6938",
        "secteurs": {i: [str(i)] for i in range(1, 10)},
    },
    {
        "code": "75056",
        "prefixe_arm": "751",
        "secteurs": {
            1: ["01", "02", "03", "04"],
            **{i: [f"{i:02d}"] for i in range(5, 21)},
        },
    },
]


@contextlib.contextmanager
def temporary_table(cursor, temp_table, reference_table, columns):
    """Context manager for creating and dropping temp tables"""

    temp_table = Identifier(temp_table)
    reference_table = Identifier(reference_table)
    columns = SQL(",").join([Identifier(c) for c in columns])

    cursor.execute(
        CREATE_TEMP_TABLE_SQL.format(
            temp_table=temp_table, reference_table=reference_table, columns=columns,
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
        columns = f.readline().strip().split(",")
        table = "data_france_codepostal_communes"
        with get_connection(using).cursor() as cursor:
            cursor.execute(
                SQL("TRUNCATE TABLE {table};").format(table=Identifier(table))
            )
            cursor.copy_expert(
                COPY_SQL.format(
                    table=Identifier(table),
                    columns=SQL(",").join(Identifier(c) for c in columns),
                ),
                f,
            )
        stderr.write(f" OK !{os.linesep}")


def importer_cantons(using):
    with open_binary("data_france.data", "cantons.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des cantons...")
        stderr.flush()
        import_with_temp_table(f, "data_france_canton", using)
        stderr.write(f" OK !{os.linesep}")


def importer_elus_municipaux(using):
    with open_binary("data_france.data", "elus_municipaux.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        stderr.write("Chargement des élus municipaux...")
        stderr.flush()
        import_with_temp_table(f, "data_france_elumunicipal", using)


def agreger_geometries_et_populations(using):
    with get_connection(using).cursor() as cursor:
        stderr.write("Calcul des géométries des secteurs électoraux...")
        stderr.flush()

        param_list = [
            {
                "arrondissements": tuple(
                    f"{ville['prefixe_arm']}{arr}" for arr in arrondissements
                ),
                "secteur": f"{ville['code']}SR{secteur:02d}",
            }
            for ville in VILLES_PLM
            for secteur, arrondissements in ville["secteurs"].items()
        ]

        cursor.executemany(
            """
            UPDATE "data_france_commune"
            SET
                geometry = (
                    SELECT ST_Multi(ST_Union(geometry :: geometry))
                    FROM "data_france_commune"
                    WHERE code IN %(arrondissements)s
                )
            WHERE code = %(secteur)s;
            """,
            param_list,
        )
        stderr.write(f" OK !{os.linesep}")

        stderr.write("Calcul des populations et géométries par département...")
        stderr.flush()
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
        stderr.write(f" OK !{os.linesep}")

        stderr.write("Calcul des populations et géométries par région...")
        stderr.flush()
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
        stderr.write(f" OK !{os.linesep}")

        stderr.write("Calcul des populations et géométries par EPCI...")
        stderr.flush()
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
        stderr.write(f" OK !{os.linesep}")


def creer_collectivites_departementales(using):
    from data_france.models import Departement, CollectiviteDepartementale, EPCI

    instances_departement = {d.code: d for d in Departement.objects.all()}
    epci_metropole_lyon = EPCI.objects.get(code="200046977")

    codes_avec_conseil_general = [
        f"{d:02d}"
        for d in range(1, 96)
        if d not in {20, 75}  # pas de conseil départemental à Paris en ou Corse
    ] + [
        "971",
        "974",
    ]  # seul la Guadeloupe et la Réunion n'ont pas de collectivité unique

    conseils_generaux = [
        {
            "code": f"{d}D",
            "type": CollectiviteDepartementale.TYPE_CONSEIL_DEPARTEMENTAL,
            "actif": True,
            "nom": f"Conseil départemental {instances_departement[d].nom_avec_charniere}",
            "departement_id": instances_departement[d].id,
        }
        for d in codes_avec_conseil_general
    ]

    metropole_lyon = {
        "code": "69M",
        "type": CollectiviteDepartementale.TYPE_CONSEIL_METROPOLE,
        "actif": True,
        "nom": "Conseil de la Métropole de Lyon",
        "departement_id": instances_departement["69"].id,
    }

    with get_connection(using).cursor() as cursor:
        stderr.write("Création des collectivités à compétences départementales...")
        stderr.flush()
        cursor.executemany(
            """
            INSERT INTO "data_france_collectivitedepartementale" ("code", "type", "actif", "departement_id", "nom")
            VALUES (%(code)s, %(type)s, %(actif)s, %(departement_id)s, %(nom)s)
            ON CONFLICT(code) DO UPDATE
            SET 
                type = excluded.type,
                actif = excluded.actif,
                departement_id = excluded.departement_id,
                nom = excluded.nom;
            """,
            conseils_generaux + [metropole_lyon],
        )

        cursor.execute(
            """
            UPDATE "data_france_collectivitedepartementale"
            SET 
                population = m.population,
                geometry = m.geometry
            FROM (
                SELECT ST_Multi(ST_Union(geometry :: geometry)) as geometry, SUM(population_municipale) AS population
                FROM "data_france_commune"
                WHERE type = 'COM' 
                AND departement_id = %(id_departement_rhone)s
                AND epci_id = %(id_epci_metropole)s
            ) AS m
            WHERE code = '69M';
            
            UPDATE "data_france_collectivitedepartementale"
            SET 
                population = m.population,
                geometry = m.geometry
            FROM (
                SELECT ST_Multi(ST_Union(geometry :: geometry)) as geometry, SUM(population_municipale) AS population
                FROM "data_france_commune"
                WHERE type = 'COM' 
                AND departement_id = %(id_departement_rhone)s
                AND (epci_id IS NULL OR epci_id != %(id_epci_metropole)s)
            ) AS m
            WHERE code = '69D';
            """,
            {
                "id_departement_rhone": instances_departement["69"].id,
                "id_epci_metropole": epci_metropole_lyon.id,
            },
        )
    stderr.write(f" OK !{os.linesep}")


def creer_collectivites_regionales(using):
    from data_france.models import Region, CollectiviteRegionale

    with open_text("data_france.data", "ctu.csv") as f:
        r = csv.DictReader(f)
        ctu = list(r)

    ctu = {c["code_region"]: c for c in ctu}

    regions = [
        (r.id, r.code, f"Conseil régional {r.nom_avec_charniere}")
        for r in Region.objects.all()
    ]

    collectivites = [
        {
            "code": ctu[code]["code"] if code in ctu else f"{code}R",
            "type": CollectiviteRegionale.TYPE_COLLECTIVITE_UNIQUE
            if code in ctu
            else CollectiviteRegionale.TYPE_CONSEIL_REGIONAL,
            "actif": True,
            "region_id": id,
            "nom": ctu[code]["nom"] if code in ctu else nom,
        }
        for id, code, nom in regions
    ]

    with get_connection(using).cursor() as cursor:
        stderr.write("Création des collectivités à compétences régionales...")
        stderr.flush()
        cursor.executemany(
            """
            INSERT INTO "data_france_collectiviteregionale" ("code", "type", "actif", "region_id", "nom")
            VALUES (%(code)s, %(type)s, %(actif)s, %(region_id)s, %(nom)s)
            ON CONFLICT(code) DO UPDATE
            SET 
                type = excluded.type,
                actif = excluded.actif,
                region_id = excluded.region_id,
                nom = excluded.nom;
            """,
            collectivites,
        )
    stderr.write(f" OK !{os.linesep}")


def creer_index_recherche(using):
    with get_connection(using).cursor() as cursor:
        stderr.write("Mise à jour de l'index de recherche...")
        cursor.execute(
            """
            WITH cps AS (
                SELECT data_france_tsvector_agg(code :: tsvector) AS codes_postaux, commune_id
                FROM data_france_codepostal AS dfcp
                INNER JOIN data_france_codepostal_communes AS dfcc
                ON dfcp.id = dfcc.codepostal_id
                GROUP BY commune_id
                
                UNION 
                
                SELECT NULL AS codes_postaux, dfc.id AS commune_id
                FROM data_france_commune dfc
                LEFT JOIN data_france_codepostal_communes dfcc 
                ON dfc.id = dfcc.commune_id
                WHERE dfcc.commune_id IS NULL
            ),
            deps AS (
                SELECT dfc.id AS commune_id, dfd.nom AS nom, dfd.code AS code FROM data_france_commune dfc
                LEFT JOIN data_france_departement dfd
                ON dfc.departement_id = dfd.id
                
                UNION
                
                SELECT dfc.id AS commune_id, dfd.nom AS nom, dfd.code AS code FROM data_france_commune dfc
                LEFT JOIN data_france_commune dfp
                ON dfc.commune_parent_id = dfp.id
                LEFT JOIN data_france_departement dfd
                ON dfp.departement_id = dfd.id
            )
            
            UPDATE data_france_commune AS dfc
            SET search = 
                setweight(to_tsvector('data_france_search' :: regconfig, dfc.nom), 'A') ||
                setweight(to_tsvector('data_france_search' :: regconfig, dfc.code), 'C') ||
                setweight(COALESCE(cps.codes_postaux, '' :: tsvector), 'B') ||
                setweight(to_tsvector(deps.code), 'C') ||
                setweight(to_tsvector('data_france_search', deps.nom), 'D')
            FROM cps, deps
            WHERE dfc.id = cps.commune_id
            AND dfc.id = deps.commune_id;
        """
        )

        cursor.execute(
            """
            WITH cps AS (
                SELECT data_france_tsvector_agg(code :: tsvector) AS codes_postaux, commune_id
                FROM data_france_codepostal AS dfcp
                INNER JOIN data_france_codepostal_communes AS dfcc
                ON dfcp.id = dfcc.codepostal_id
                GROUP BY commune_id
                
                UNION 
                
                SELECT NULL AS codes_postaux, dfc.id AS commune_id
                FROM data_france_commune dfc
                LEFT JOIN data_france_codepostal_communes dfcc 
                ON dfc.id = dfcc.commune_id
                WHERE dfcc.commune_id IS NULL
            ),
            deps AS (
                SELECT dfc.id AS commune_id, dfd.nom AS nom, dfd.code AS code FROM data_france_commune dfc
                LEFT JOIN data_france_departement dfd
                ON dfc.departement_id = dfd.id
            )
            
            UPDATE data_france_elumunicipal em
            SET search =
                   setweight(to_tsvector('data_france_search', COALESCE(em."nom", '')), 'A')
                || setweight(to_tsvector('data_france_search', COALESCE(em."prenom", '')), 'A')
                || setweight(to_tsvector('data_france_search', COALESCE(c."nom", '')), 'B')
                || setweight(COALESCE(cps.codes_postaux, '' :: tsvector), 'C')
                || setweight(to_tsvector(deps.code), 'C')
                || setweight(to_tsvector('data_france_search', deps.nom), 'D')
            FROM data_france_commune c, cps, deps
            WHERE c.id = em.commune_id AND c.id = cps.commune_id AND c.id = deps.commune_id;"""
        )

        stderr.write(f" OK !{os.linesep}")


def import_with_temp_table(csv_file, table, using):
    temp_table = f"{table}_temp"
    columns = csv_file.readline().strip().split(",")

    with get_connection(using).cursor() as cursor, temporary_table(
        cursor, temp_table, table, columns
    ):

        cursor.copy_expert(
            COPY_SQL.format(
                table=Identifier(temp_table),
                columns=SQL(",").join(Identifier(c) for c in columns),
            ),
            csv_file,
        )

        cursor.execute(
            COPY_FROM_TEMP_TABLE.format(
                table=Identifier(table),
                temp_table=Identifier(temp_table),
                all_columns=SQL(",").join(Identifier(c) for c in columns),
                id_column=Identifier(columns[0]),
                setters=SQL(",").join(
                    Identifier(c) + SQL(" = ") + Identifier("excluded", c)
                    for c in columns[1:]
                ),
            ),
        )


def importer_donnees(using=None):
    auto_commit = transaction.get_autocommit(using=using)
    if not auto_commit:
        transaction.set_autocommit(True, using=using)

    try:
        # à importer avant les communes
        importer_epci(using)

        # ces trois tables ont des foreign key croisées
        # Django crée les contraintes de clés étrangères
        # en mode "différable", ce qui permet d'importer
        # facilement ces tables en les groupant dans une
        # transaction
        with transaction.atomic():
            import_regions(using)
            import_departements(using)
            importer_communes(using)

        importer_codes_postaux(using)

        importer_associations_communes_codes_postaux(using)

        importer_cantons(using)

        importer_elus_municipaux(using)

        agreger_geometries_et_populations(using)

        creer_collectivites_departementales(using)

        creer_collectivites_regionales(using)

        creer_index_recherche(using)

    finally:
        if not auto_commit:
            transaction.set_autocommit(False, using=using)
