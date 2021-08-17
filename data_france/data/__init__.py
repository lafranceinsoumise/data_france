import contextlib
import csv
import lzma
import os
from dataclasses import dataclass
from importlib.resources import open_binary, open_text
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
    INSERT INTO {table} ({all_columns})
    SELECT {all_columns}
    FROM {temp_table}
    ON CONFLICT({id_column}) DO UPDATE SET {setters}
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


@console_message("Chargement des régions")
def import_regions(using):
    with open_binary("data_france.data", "regions.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_region", using)


@console_message("Chargements des départements")
def import_departements(using):
    with open_binary("data_france.data", "departements.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_departement", using)


@console_message("Chargement des EPCI")
def importer_epci(using):
    with open_binary("data_france.data", "epci.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_epci", using)


@console_message("Chargement des communes")
def importer_communes(using):
    with open_binary("data_france.data", "communes.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_commune", using)


@console_message("Chargement des codes postaux")
def importer_codes_postaux(using):
    with open_binary("data_france.data", "codes_postaux.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_codepostal", using)


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


@console_message("Chargement des cantons")
def importer_cantons(using):
    with open_binary("data_france.data", "cantons.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_canton", using)


@console_message("Chargement des circonscriptions consulaires")
def importer_circonscriptions_consulaires(using):
    with open_binary(
        "data_france.data", "circonscriptions_consulaires.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
        import_with_temp_table(f, "data_france_circonscriptionconsulaire", using)


@console_message("Chargement des circonscriptions législatives")
def importer_circonscriptions_legislatives(using):
    with open_binary(
        "data_france.data", "circonscriptions_legislatives.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
        import_with_temp_table(f, "data_france_circonscriptionlegislative", using)


@console_message("Chargement des élus municipaux")
def importer_elus_municipaux(using):
    with open_binary("data_france.data", "elus_municipaux.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_elumunicipal", using)


@console_message("Chargement des élus départementaux")
def importer_elus_departementaux(using):
    with open_binary(
        "data_france.data", "elus_departementaux.csv.lzma"
    ) as _f, lzma.open(_f, "rt") as f:
        import_with_temp_table(f, "data_france_eludepartemental", using)


@console_message("Chargement des élus régionaux")
def importer_elus_regionaux(using):
    with open_binary("data_france.data", "elus_regionaux.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_eluregional", using)


@console_message("Chargement des députés")
def importer_deputes(using):
    with open_binary("data_france.data", "deputes.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_depute", using)


@console_message("Chargement des députés européens")
def importer_deputes_europeens(using):
    with open_binary("data_france.data", "deputes_europeens.csv.lzma") as _f, lzma.open(
        _f, "rt"
    ) as f:
        import_with_temp_table(f, "data_france_deputeeuropeen", using)


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


@console_message("Création des collectivités à compétences départementales")
def creer_collectivites_departementales(using):
    from data_france.models import Departement, CollectiviteDepartementale, EPCI

    instances_departement = {d.code: d for d in Departement.objects.all()}
    epci_metropole_lyon = EPCI.objects.get(code="200046977")

    codes_avec_conseil_general = [
        f"{d:02d}"
        for d in range(1, 96)
        if d
        not in {
            20,
            67,
            68,
            75,
        }  # pas de conseil départemental à Paris, en Alsace, en Corse
    ] + ["971", "974"]
    # seules la Guadeloupe et la Réunion n'ont pas de collectivité unique

    conseils_departementaux = [
        {
            "code": f"{d}D",
            "type": CollectiviteDepartementale.TYPE_CONSEIL_DEPARTEMENTAL,
            "actif": True,
            "nom": f"Conseil départemental {instances_departement[d].nom_avec_charniere}",
            "type_nom": TypeNom.ARTICLE_LE,
            "region_id": instances_departement[d].region_id,
        }
        for d in codes_avec_conseil_general
    ]

    lyon = {
        "code": "69M",
        "type": CollectiviteDepartementale.TYPE_STATUT_PARTICULIER,
        "actif": True,
        "nom": "Métropole de Lyon",
        "type_nom": TypeNom.ARTICLE_LA,
        "region_id": instances_departement["69"].region_id,
    }

    paris = {
        "code": "75C",
        "type": CollectiviteDepartementale.TYPE_STATUT_PARTICULIER,
        "actif": True,
        "nom": "Commune de Paris",
        "type_nom": TypeNom.ARTICLE_LA,
        "region_id": instances_departement["75"].region_id,
    }

    alsace = {
        "code": "6AE",
        "type": CollectiviteDepartementale.TYPE_CONSEIL_DEPARTEMENTAL,
        "actif": True,
        "nom": "Collectivité européenne d'Alsace",
        "type_nom": TypeNom.ARTICLE_LA,
        "region_id": instances_departement["67"].region_id,
    }

    martinique = {
        "code": "972R",
        "type": CollectiviteDepartementale.TYPE_STATUT_PARTICULIER,
        "actif": True,
        "nom": "Collectivité unique de Martinique",
        "type_nom": TypeNom.ARTICLE_LA,
        "region_id": instances_departement["972"].region_id,
    }

    guyane = {
        "code": "973R",
        "type": CollectiviteDepartementale.TYPE_STATUT_PARTICULIER,
        "actif": True,
        "nom": "Collectivité unique de Guyane",
        "type_nom": TypeNom.ARTICLE_LA,
        "region_id": instances_departement["973"].region_id,
    }

    mayotte = {
        "code": "976R",
        "type": CollectiviteDepartementale.TYPE_STATUT_PARTICULIER,
        "actif": True,
        "nom": "Conseil départemental de Mayotte",
        "type_nom": TypeNom.ARTICLE_LE,
        "region_id": instances_departement["976"].region_id,
    }

    corse = {
        "code": "20R",
        "type": CollectiviteDepartementale.TYPE_STATUT_PARTICULIER,
        "actif": True,
        "nom": "Collectivité de Corse",
        "type_nom": TypeNom.ARTICLE_LA,
        "region_id": instances_departement["2A"].region_id,
    }

    with get_connection(using).cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO "data_france_collectivitedepartementale"
                ("code", "type", "actif", "region_id", "nom", "type_nom")
            VALUES (%(code)s, %(type)s, %(actif)s, %(region_id)s, %(nom)s, %(type_nom)s)
            ON CONFLICT(code) DO UPDATE
            SET
                type = excluded.type,
                actif = excluded.actif,
                region_id = excluded.region_id,
                nom = excluded.nom,
                type_nom = excluded.type_nom;
            """,
            conseils_departementaux
            + [lyon, paris, alsace, martinique, guyane, mayotte, corse],
        )

        cursor.execute(
            """
            UPDATE "data_france_collectivitedepartementale" c
            SET
                population = d.population,
                geometry = d.geometry
            FROM "data_france_departement" d
            WHERE d.code = TRIM(trailing 'DRC' from c.code)
            AND c.code IN %(codes)s
            """,
            {
                "codes": tuple(
                    c["code"]
                    for c in (
                        conseils_departementaux + [paris, martinique, guyane, mayotte]
                    )
                )
            },
        )

        cursor.execute(
            """
            UPDATE "data_france_collectivitedepartementale"
            SET
                population = m.population,
                geometry = m.geometry
            FROM (
                SELECT
                    SUM(population_municipale) AS population,
                    ST_Multi(ST_Union(geometry :: geometry)) as geometry
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


@console_message("Création des collectivités à compétences régionales")
def creer_collectivites_regionales(using):
    from data_france.models import Region, CollectiviteRegionale

    with open_text("data_france.data", "ctu.csv") as f:
        r = csv.DictReader(f)
        ctu = list(r)

    ctu = {c["code_region"]: c for c in ctu}

    regions = [
        (r.id, r.code, r.nom_avec_charniere, r.type_nom) for r in Region.objects.all()
    ]

    collectivites = [
        {
            "code": ctu[code]["code"] if code in ctu else f"{code}R",
            "type": CollectiviteRegionale.TYPE_COLLECTIVITE_UNIQUE
            if code in ctu
            else CollectiviteRegionale.TYPE_CONSEIL_REGIONAL,
            "actif": True,
            "region_id": id,
            "nom": ctu[code]["nom"] if code in ctu else f"Conseil régional {nom}",
            "type_nom": ctu[code]["type_nom"] if code in ctu else type_nom,
        }
        for id, code, nom, type_nom in regions
    ]

    with get_connection(using).cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO "data_france_collectiviteregionale" ("code", "type", "actif", "region_id", "nom", "type_nom")
            VALUES (%(code)s, %(type)s, %(actif)s, %(region_id)s, %(nom)s, %(type_nom)s)
            ON CONFLICT(code) DO UPDATE
            SET
                type = excluded.type,
                actif = excluded.actif,
                region_id = excluded.region_id,
                nom = excluded.nom,
                type_nom = excluded.type_nom;
            """,
            collectivites,
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
        FROM data_france_region r
        WHERE e.region_id = r.id
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

        importer_circonscriptions_consulaires(using)

        importer_circonscriptions_legislatives(using)

        importer_elus_municipaux(using)

        importer_elus_departementaux(using)

        importer_elus_regionaux(using)

        importer_deputes(using)

        importer_deputes_europeens(using)

        agreger_geometries_et_populations(using)

        creer_collectivites_departementales(using)

        creer_collectivites_regionales(using)

        creer_index_recherche(using)

    finally:
        if not auto_commit:
            transaction.set_autocommit(False, using=using)
