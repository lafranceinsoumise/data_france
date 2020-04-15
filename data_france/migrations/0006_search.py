from django.db import migrations

install_unaccent = "CREATE EXTENSION IF NOT EXISTS unaccent;"

create_search_config = """
CREATE TEXT SEARCH CONFIGURATION data_france_search ( COPY = simple );
ALTER TEXT SEARCH CONFIGURATION data_france_search
  ALTER MAPPING FOR hword, hword_part, word
  WITH unaccent, simple;
"""

delete_search_config = "DROP TEXT SEARCH CONFIGURATION data_france_search;"

add_search_index = """
CREATE INDEX data_france_commune_search_index ON data_france_commune USING GIN ((
    setweight(to_tsvector('data_france_search', COALESCE("nom", '')), 'A')
    || setweight(to_tsvector('data_france_search', COALESCE("code", '')), 'B')
));
"""

drop_search_index = "DROP INDEX data_france_commune_search_index"


class Migration(migrations.Migration):
    dependencies = [
        ("data_france", "0005_auto_20200330_0827"),
    ]

    operations = [
        migrations.RunSQL(sql=install_unaccent, reverse_sql=migrations.RunSQL.noop),
        migrations.RunSQL(sql=create_search_config, reverse_sql=delete_search_config),
        migrations.RunSQL(sql=add_search_index, reverse_sql=drop_search_index),
    ]
