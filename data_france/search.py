import re

from django.contrib.postgres.search import SearchQuery

# taken from django-watson: https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L26
RE_POSTGRES_ESCAPE_CHARS = re.compile(r"[&:(|)!><'-]", re.UNICODE)

# inspired from django-watson:
# https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L33
# https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L186


class PrefixSearchQuery(SearchQuery):
    def as_sql(self, compiler, connection):
        params = [self.parse_query_text(self.value)]
        if self.config:
            config_sql, config_params = compiler.compile(self.config)
            template = "to_tsquery({}::regconfig, %s)".format(config_sql)
            params = config_params + params
        else:
            template = "to_tsquery(%s)"
        if self.invert:
            template = f"!!({template})"

        return template, params

    def parse_query_text(self, text):
        """
        normalizes the query text to a format that can be consumed
        by the backend database
        """
        text = RE_POSTGRES_ESCAPE_CHARS.sub(
            " ", text
        ).strip()  # Replace harmful characters with space.

        if not text:
            return ""

        words = ["$${0}$$".format(word) for word in text.split()]

        if not words:
            return ""

        # qualifying a term with :* means searching it as a prefix
        words[-1] = f"{words[-1]}:*"
        return " & ".join(words)
