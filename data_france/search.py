import re

from django.contrib.postgres.search import SearchQuery

# taken from django-watson: https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L26
RE_POSTGRES_ESCAPE_CHARS = re.compile(r"[&:(|)!><]", re.UNICODE)
RE_SPACE = re.compile(r"\s+", re.UNICODE)


# inspired from django-watson:
# https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L33
# https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L186


def escape_prefix_query(text, re_escape_chars):
    """
    normalizes the query text to a format that can be consumed
    by the backend database
    """
    text = re_escape_chars.sub(" ", text)  # Replace harmful characters with space.

    # qualifying a term with :* means searching it as a prefix
    return " & ".join("$${0}$$:*".format(word) for word in text.split())


class PrefixSearchQuery(SearchQuery):
    def as_sql(self, compiler, connection):
        params = [escape_prefix_query(self.value, RE_POSTGRES_ESCAPE_CHARS)]
        if self.config:
            config_sql, config_params = compiler.compile(self.config)
            template = "to_tsquery({}::regconfig, %s)".format(config_sql)
            params = config_params + params
        else:
            template = "to_tsquery(%s)"
        if self.invert:
            template = f"!!({template})"

        return template, params
