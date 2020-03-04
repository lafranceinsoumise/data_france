import datetime
from dataclasses import dataclass
from pathlib import PurePath
from typing import List


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
class SourceVersion:
    url: str
    date: Date
    hash: str

    @property
    def filename(self):
        suffix = PurePath(self.url).suffix
        if self.date:
            return f"{str(self.date)}{suffix}"
        return f"inconnue{suffix}"


@dataclass
class Source:
    path: PurePath
    versions: List[SourceVersion]

    @property
    def url(self):
        return self.versions[0].url

    @property
    def filename(self):
        return self.path / self.versions[0].filename

    @property
    def hash(self):
        return self.versions[0].hash


def iterate_sources(sources):
    stack = [(PurePath(""), sources)]
    while stack:
        path, value = stack.pop()
        if isinstance(value, dict):
            stack.extend((path / str(p), s) for p, s in value.items())

        else:
            yield Source(
                path,
                [
                    SourceVersion(s["url"], parse_date(s["date"]), s["hash"])
                    for s in value
                ],
            )
