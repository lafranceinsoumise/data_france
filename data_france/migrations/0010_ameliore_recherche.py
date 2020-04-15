from django.db import migrations
import django.contrib.postgres.search

better_search_config = """
ALTER TEXT SEARCH CONFIGURATION "data_france_search"
  ALTER MAPPING FOR asciihword, asciiword, hword, hword_asciipart, hword_part, word
  WITH unaccent, french_stem;
COMMENT ON TEXT SEARCH CONFIGURATION "data_france_search"
IS 'Configuration de recherche pour les communes (data_france)';
"""

reset_search_config = """
ALTER TEXT SEARCH CONFIGURATION data_france_search
  ALTER MAPPING FOR hword, hword_part, word
  WITH unaccent, simple;
ALTER TEXT SEARCH CONFIGURATION data_france_search
  ALTER MAPPING FOR asciihword, asciiword, hword_asciipart
  WITH simple;
"""

set_up_new_search_index = """
DROP INDEX data_france_commune_search_index;
CREATE INDEX data_france_commune_search_index ON data_france_commune USING GIN ("search");
"""

reset_search_index = """
DROP INDEX data_france_commune_search_index;
CREATE INDEX data_france_commune_search_index ON data_france_commune USING GIN ((
    setweight(to_tsvector('data_france_search', COALESCE("nom", '')), 'A')
    || setweight(to_tsvector('data_france_search', COALESCE("code", '')), 'B')
));
"""

create_tsvector_agg_func = """
CREATE AGGREGATE data_france_tsvector_agg (
    BASETYPE = pg_catalog.tsvector,
    STYPE = pg_catalog.tsvector,
    SFUNC = pg_catalog.tsvector_concat,
    INITCOND = ''
);
"""

delete_tsvector_agg_func = """
DROP AGGREGATE data_france_tsvector_agg ( tsvector );
"""


class Migration(migrations.Migration):
    dependencies = [("data_france", "0009_codes_postaux")]

    operations = [
        migrations.RunSQL(sql=better_search_config, reverse_sql=reset_search_config),
        migrations.AddField(
            model_name="commune",
            name="search",
            field=django.contrib.postgres.search.SearchVectorField(
                editable=False, null=True, verbose_name="Champ de recherche"
            ),
        ),
        migrations.RunSQL(sql=set_up_new_search_index, reverse_sql=reset_search_index,),
        migrations.RunSQL(
            sql=create_tsvector_agg_func, reverse_sql=delete_tsvector_agg_func
        ),
    ]
