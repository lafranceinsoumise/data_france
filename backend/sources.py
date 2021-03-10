import datetime
from dataclasses import dataclass
from pathlib import Path, PurePath

import yaml


BASE_DIR = Path(__file__).parent.parent
SOURCE_DIR = BASE_DIR / "sources"
PREPARE_DIR = BASE_DIR / "build"


with open(BASE_DIR / "sources.yml") as f:
    _sources = yaml.load(f, yaml.BaseLoader)


class SourceExplorer:
    def __init__(self, d, p: PurePath):
        self._d = d
        self._p = p

    def __getattr__(self, item):
        if item not in self._d:
            raise AttributeError(f"Attribut `{item}' manquant")

        if "url" in self._d[item]:
            return Source(self._p / item, **self._d[item])

        return SourceExplorer(self._d[item], self._p / item)

    def __iter__(self):
        stack = [(self._p, self._d)]
        while stack:
            path, value = stack.pop()
            if "url" in value:
                yield Source(path, **value)
            else:
                stack.extend((path / str(p), s) for p, s in value.items())


SOURCES = SourceExplorer(_sources, PurePath(""))


def iterate_sources():
    stack = [(PurePath(""), _sources)]
    while stack:
        path, value = stack.pop()
        if "url" in value:
            yield Source(path, **value)
        else:
            stack.extend((path / str(p), s) for p, s in value.items())


def parse_date(d):
    try:
        return Date(datetime.datetime.strptime(d, "%d/%m/%Y").date(), False)
    except ValueError:
        pass
    try:
        return Date(datetime.datetime.strptime(d, "%m/%Y").date(), True)
    except ValueError:
        return None


@dataclass(order=True)
class Date:
    date: datetime.date
    monthly: bool

    def __str__(self):
        if self.monthly:
            return self.date.strftime("%Y-%m")
        return self.date.strftime("%Y-%m-%d")


@dataclass
class Source:
    path: PurePath
    url: str
    hash: str
    date: Date = None
    extension: str = None
    delimiter: str = ";"
    corrected: str = None

    @property
    def suffix(self):
        if self.extension is not None:
            return self.extension
        return PurePath(self.url).suffix

    @property
    def filename(self):
        return self.path.parent / (self.path.name + self.suffix)
