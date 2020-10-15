import re

from django.contrib.postgres.search import SearchQuery

# taken from django-watson: https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L26
RE_POSTGRES_ESCAPE_CHARS = re.compile(r"[&:(|)!><'-]", re.UNICODE)

# inspired from django-watson:
# https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L33
# https://github.com/etianen/django-watson/blob/2226de139b6e177bfbe2824b1749478dbcce3318/watson/backends.py#L186


class PrefixSearchQuery(SearchQuery):
    def __init__(self, value, output_field=None, *, config=None, invert=False):
        value = self.parse_query_text(value)
        super().__init__(
            value, output_field, config=config, invert=invert, search_type="raw"
        )

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
