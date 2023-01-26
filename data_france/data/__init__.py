import contextlib
import lzma
import os
from dataclasses import dataclass
from importlib.resources import open_binary
from sys import stderr
from typing import Tuple

from django.db import transaction
from django.db.transaction import get_connection
from psycopg2.sql import SQL, Identifier

from data_france.utils import TypeNom

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
    INSERT INTO {table} ({column_list})
    SELECT {select_list}
    FROM {temp_table}
    ON CONFLICT({id_column}) DO UPDATE SET {setters};
    """
)

MARQUER_INACTIF = SQL(
    """
    UPDATE {table}
    SET actif = false
    WHERE actif = true AND id NOT IN (
        SELECT id FROM {temp_table}
    );
    """
)


@dataclass
class SecteurPLM:
    code: str
    arrondissements: Tuple[str]
    nom: str


@dataclass
class VillePLM:
    code: str
    nom: str
    type_nom: int
    arrondissements: Tuple[str]
    secteurs: Tuple[SecteurPLM]

    def __init__(self, code, nom, type_nom, prefixe_arm, secteurs):
        self.code = code
        self.nom = nom
        self.type_nom = type_nom

        self.secteurs = tuple(
            SecteurPLM(
                f"{code}SR{num:02d}",
                tuple(f"{prefixe_arm}{arr}" for arr in arrs),
                f"{self.nom} — {nom}",
            )
            for num, arrs, nom in secteurs
        )

        self.arrondissements = tuple(
            sorted(arr for s in self.secteurs for arr in s.arrondissements)
        )


VILLES_PLM = [
    VillePLM(
        "13055",
        "Marseille",
        TypeNom.CONSONNE,
        "132",
        [
            (1, ["01", "07"], "1er secteur"),
            (2, ["02", "03"], "2e secteur"),
            (3, ["04", "05"], "3e secteur"),
            (4, ["06", "08"], "4e secteur"),
            (5, ["09", "10"], "5e secteur"),
            (6, ["11", "12"], "6e secteur"),
            (7, ["13", "14"], "7e secteur"),
            (8, ["15", "16"], "8e secteur"),
        ],
    ),
    VillePLM(
        "69123",
        "Lyon",
        TypeNom.CONSONNE,
        "6938",
        [
            (1, ["1"], "1er arrondissement"),
            *((i, [str(i)], f"{i}e arrondissement") for i in range(2, 10)),
        ],
    ),
    VillePLM(
        "75056",
        "Paris",
        TypeNom.CONSONNE,
        "751",
        [
            (1, ["01", "02", "03", "04"], "centre"),
            *((i, [f"{i:02d}"], f"{i}e arrondissement") for i in range(5, 21)),
        ],
    ),
]


@contextlib.contextmanager
def console_message(message):
    stderr.write(
        f"{message}... ",
    )
    stderr.flush()

    yield
    stderr.write(f"OK!{os.linesep}")


@contextlib.contextmanager
def temporary_table(cursor, temp_table, reference_table, columns):
    """Context manager for creating and dropping temp tables"""

    temp_table = Identifier(temp_table)
    reference_table = Identifier(reference_table)
    columns = SQL(",").join([Identifier(c) for c in columns])

    cursor.execute(
        CREATE_TEMP_TABLE_SQL.format(
            temp_table=temp_table,
            reference_table=reference_table,
            columns=columns,
        )
    )

    try:
        yield
    finally:
        cursor.execute(DROP_TEMPORARY_TABLE_SQL.format(temp_table=temp_table))


@console_message("Chargement des associations Communes/Codes postaux")
def importer_associations_communes_codes_postaux(using):
    with open_binary(
        "data_france.data", "codes_postaux_communes.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
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


def agreger_geometries_et_populations(using):
    with get_connection(using).cursor() as cursor:

        param_list = [
            {
                "arrondissements": secteur.arrondissements,
                "secteur": secteur.code,
            }
            for ville in VILLES_PLM
            for secteur in ville.secteurs
        ]

        with console_message("Calcul des géométries des secteurs électoraux"):
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

        with console_message("Calcul des populations et géométries par département"):
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

        with console_message("Calcul des populations et géométries par région"):
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

        with console_message("Calcul des populations et géométries par EPCI"):
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

        with console_message("Calcul des populations et géométries des collectivités départementales"):
            # les conseils généraux et collectivités départementales qui correspondent à des départements
            # cela inclut tous les conseils départementaux plus certaines collectivités uniques dont Paris (75C),
            # les collectivités uniques d'outremer (972R, 973R, 976D)
            # par contre il faut exclure le Rhône qui est un cas particulier
            cursor.execute(
                """
                UPDATE "data_france_collectivitedepartementale" c
                SET
                    population = d.population,
                    geometry = d.geometry
                FROM "data_france_departement" d
                WHERE d.code = TRIM(trailing 'DRC' from c.code)
                AND d.code != '69D'
                """,
            )

            # Le Rhône et la métropole de Lyon sont un cas particulier
            cursor.execute(
                """
                UPDATE "data_france_collectivitedepartementale"
                SET
                    population = m.population,
                    geometry = m.geometry
                FROM (
                    SELECT
                        SUM(population_municipale) AS population,
                        ST_Multi(ST_Union(com.geometry :: geometry)) as geometry
                    FROM "data_france_commune" com
                    JOIN "data_france_epci" epci ON com.epci_id = epci.id
                    JOIN "data_france_departement" dep ON com.departement_id = dep.id
                    WHERE com.type = 'COM'
                    AND dep.code = %(code_dep)s
                    AND epci.code = %(code_metropole)s
                ) AS m
                WHERE code = '69M';
    
                UPDATE "data_france_collectivitedepartementale"
                SET
                    population = m.population,
                    geometry = m.geometry
                FROM (
                    SELECT
                        SUM(population_municipale) AS population,
                        ST_Multi(ST_Union(com.geometry :: geometry)) as geometry
                    FROM "data_france_commune" com
                    LEFT JOIN "data_france_epci" epci ON com.epci_id = epci.id
                    JOIN "data_france_departement" dep ON com.departement_id = dep.id                    
                    WHERE com.type = 'COM'
                    AND dep.code = %(code_dep)s
                    AND (epci.code IS NULL OR epci.code != %(code_metropole)s)
                ) AS m
                WHERE code = '69D';
                """,
                {
                    "code_dep": "69",
                    "code_metropole": "200046977",
                },
            )

            # la collectivité européenne d'Alsace et l'Assemblée de Corse sont un cas particulier
            cursor.executemany(
                """
                UPDATE "data_france_collectivitedepartementale" c
                SET
                    population = m.population,
                    geometry = m.geometry
                FROM (
                    SELECT
                       SUM(population) AS population,
                       ST_Multi(ST_Union(geometry :: geometry)) as geometry
                    FROM "data_france_departement"
                    WHERE code IN %(codes_d)s
                ) m
                WHERE code = %(code_c)s;
                """,
                [
                    {"code_c": "6AE", "codes_d": ("67", "68")},
                    {"code_c": "20R", "codes_d": ("2A", "2B")},
                ],
            )

@console_message("Mise à jour de l'index de recherche")
def creer_index_recherche(using):
    with get_connection(using).cursor() as cursor:
        # L'index des communes
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

        # L'index des circonscriptions consulaires
        cursor.execute(
            """
        UPDATE data_france_circonscriptionconsulaire c
        SET search =
            setweight(to_tsvector('data_france_search', COALESCE(c.nom, '')), 'A')
         || setweight(to_tsvector('data_france_search', ARRAY_TO_STRING(c.consulats, ' ')), 'B');
"""
        )

        # l'index des élus municipaux
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

        # l'index des élus départementaux
        cursor.execute(
            """
        UPDATE data_france_eludepartemental e
        SET search = setweight(to_tsvector('data_france_search', COALESCE(e."nom", '')), 'A')
         || setweight(to_tsvector('data_france_search', COALESCE(e."prenom", '')), 'A')
         || setweight(to_tsvector('data_france_search', COALESCE(c."nom", '')), 'B')
         || setweight(to_tsvector('data_france_search', COALESCE(d."nom", '')), 'C')
         || setweight(to_tsvector('data_france_search', COALESCE(d."code", '')), 'C')
        FROM data_france_canton c, data_france_departement d
        WHERE c.id = e.canton_id
          AND d.id = c.departement_id
        """
        )

        # l'index des élus régionaux
        cursor.execute(
            """
        UPDATE data_france_eluregional e
        SET search =
            setweight(to_tsvector('data_france_search', COALESCE(e."nom", '')), 'A')
         || setweight(to_tsvector('data_france_search', COALESCE(e."prenom", '')), 'A')
         || setweight(to_tsvector('data_france_search', COALESCE(r."nom", '')), 'B')
        FROM data_france_collectiviteregionale cr
        LEFT JOIN data_france_region r ON cr.region_id = r.id
        WHERE e.collectivite_regionale_id = cr.id
        """
        )

        # l'index des députés
        # Il y a des circonscriptions sans départements (français de l'étranger
        # et collectivités d'outremer), d'où l'obligation de recourir à une
        # sous-requête pour pouvoir faire une jointure à gauche
        cursor.execute(
            """
        WITH circonscription AS (
          SELECT
            c.id AS id,
            setweight(to_tsvector('data_france_search', COALESCE(c."code", '')), 'C')
         || setweight(to_tsvector('data_france_search', COALESCE(d."nom", '')), 'C')
            AS search
          FROM data_france_circonscriptionlegislative c
          LEFT JOIN data_france_departement d ON c.departement_id = d.id
        )
        UPDATE data_france_depute e
        SET search =
            setweight(to_tsvector('data_france_search', COALESCE(e."nom", '')), 'A')
         || setweight(to_tsvector('data_france_search', COALESCE(e."prenom", '')), 'A')
         || c.search
        FROM circonscription c
        WHERE c.id = e.circonscription_id
        """
        )

        # l'index des députés européens
        cursor.execute(
            """
        UPDATE data_france_deputeeuropeen
        SET search =
            setweight(to_tsvector('data_france_search', COALESCE("nom", '')), 'A')
         || setweight(to_tsvector('data_france_search', COALESCE("prenom", '')), 'A')
        """
        )


def import_with_temp_table(csv_file, table, using, marquer_inactif=False):
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

        setters = [
            Identifier(c) + SQL(" = ") + Identifier("excluded", c) for c in columns[1:]
        ]
        column_list = [Identifier(c) for c in columns]
        select_list = column_list

        if marquer_inactif:
            select_list = [*column_list, SQL("false AS ") + Identifier("actif")]
            column_list = [*column_list, Identifier("actif")]
            setters.append(Identifier("actif") + SQL(" = true"))

        cursor.execute(
            COPY_FROM_TEMP_TABLE.format(
                table=Identifier(table),
                temp_table=Identifier(temp_table),
                column_list=SQL(",").join(column_list),
                select_list=SQL(",").join(select_list),
                id_column=Identifier(columns[0]),
                setters=SQL(",").join(setters),
            ),
        )

        if marquer_inactif:
            cursor.execute(
                MARQUER_INACTIF.format(
                    table=Identifier(table), temp_table=Identifier(temp_table)
                )
            )


def import_standard(lzma_file, table, message, marquer_inactif=False, using=None):
    with console_message(message):
        with open_binary("data_france.data", lzma_file) as _f, lzma.open(
                    _f, "rt"
            ) as f:
                import_with_temp_table(f, table, marquer_inactif=marquer_inactif, using=using)


def importer_donnees(using=None):
    auto_commit = transaction.get_autocommit(using=using)
    if not auto_commit:
        transaction.set_autocommit(True, using=using)

    try:
        # à importer avant les communes
        import_standard(
            "epci.csv.lzma",
            "data_france_epci",
            "Chargement des EPCI",
            using=using
        )

        # ces trois tables ont des foreign key croisées
        # Django crée les contraintes de clés étrangères
        # en mode "différable", ce qui permet d'importer
        # facilement ces tables en les groupant dans une
        # transaction
        with transaction.atomic():
            import_standard(
                "regions.csv.lzma",
                "data_france_region",
                "Chargement des régions",
                using=using
            )
            import_standard(
                "departements.csv.lzma",
                "data_france_departement",
                "Chargement des départements",
                using=using
            )
            import_standard(
                "communes.csv.lzma",
                "data_france_commune",
                "Chargement des communes",
                using=using
            )

        import_standard(
            "collectivites_departementales.csv.lzma",
            "data_france_collectivitedepartementale",
            "Chargement des collectivités départementales",
            marquer_inactif=True,
            using=using
        )

        import_standard(
            "collectivites_regionales.csv.lzma",
            "data_france_collectiviteregionale",
            "Chargement des collectivités régionales",
            marquer_inactif=True,
            using=using
        )

        import_standard(
            "codes_postaux.csv.lzma",
            "data_france_codepostal",
            "Chargement des codes postaux",
            using=using
        )

        importer_associations_communes_codes_postaux(using)

        import_standard(
            "cantons.csv.lzma",
            "data_france_canton",
            "Chargement des cantons",
            using=using
        )

        import_standard(
            "circonscriptions_consulaires.csv.lzma",
            "data_france_circonscriptionconsulaire",
            "Chargement des circonscriptions consulaires",
            using=using
        )

        import_standard(
            "circonscriptions_legislatives.csv.lzma",
            "data_france_circonscriptionlegislative",
            "Chargement des circonscriptions législatives",
            using=using
        )

        import_standard(
            "elus_municipaux.csv.lzma",
            "data_france_elumunicipal",
            "Chargement des élus municipaux",
            marquer_inactif=True,
            using=using
        )

        import_standard(
            "elus_departementaux.csv.lzma",
            "data_france_eludepartemental",
            "Chargement des élus départementaux",
            marquer_inactif=True,
            using=using
        )

        import_standard(
            "elus_regionaux.csv.lzma",
            "data_france_eluregional",
            "Chargement des élus régionaux",
            marquer_inactif=True,
            using=using
        )

        import_standard(
            "deputes.csv.lzma",
            "data_france_depute",
            "Chargement des députés",
            marquer_inactif=True,
            using=using
        )

        import_standard(
            "deputes_europeens.csv.lzma",
            "data_france_deputeeuropeen",
            "Chargement des députés européens",
            marquer_inactif=True,
            using=using
        )

        agreger_geometries_et_populations(using)

        creer_index_recherche(using)

    finally:
        if not auto_commit:
            transaction.set_autocommit(False, using=using)
