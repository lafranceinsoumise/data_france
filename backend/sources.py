import datetime
from dataclasses import dataclass
from pathlib import PurePath
from typing import List, Union


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
    date: Date
    hash: str
    preparation: str = None
    targets: Union[str, List[str]] = None

    @property
    def filename(self):
        suffix = PurePath(self.url).suffix
        return self.path.parent / (self.path.name + suffix)

    def __getattr__(self, item):
        return getattr(self.versions[0], item)


def iterate_sources(sources):
    stack = [(PurePath(""), sources)]
    while stack:
        path, value = stack.pop()
        if "url" in value:
            yield Source(path, **value)
        else:
            stack.extend((path / str(p), s) for p, s in value.items())
