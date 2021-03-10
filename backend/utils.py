import hashlib
import re
import unicodedata
from collections import deque
from pathlib import Path, PurePath
from zipfile import ZipFile
from libarchive.public import file_reader as archive_reader

import pandas as pd

BLOCKSIZE = 65536


class check_hash:
    def __init__(self, filename: Path, hash_digest):
        self.filename = filename
        self.hash_digest = hash_digest

    def _check(self):
        hasher = hashlib.new("sha256")

        if not self.filename.exists():
            return False

        with self.filename.open("rb") as f:
            buf = f.read(BLOCKSIZE)
            while buf:
                hasher.update(buf)
                buf = f.read(BLOCKSIZE)

        return hasher.hexdigest() == self.hash_digest

    def _check_after_run(self):
        if not self._check():
            raise RuntimeError(f"Hash incorrect pour '{self.filename}'")

        return {}

    def __call__(self, task, values):
        task.value_savers.append(self._check_after_run)
        return self._check()


def get_zip_targets(zip_path, dest_prefix):
    zip_file = ZipFile(zip_path)
    info_list = zip_file.infolist()
    if len(info_list) == 1:
        return [dest_prefix.with_suffix(PurePath(info_list[0].filename).suffix)]

    else:
        return [dest_prefix / info.filename for info in info_list]


def extract_zip_file(zip_path, dest_prefix: Path):
    zip_file = ZipFile(zip_path)
    if len(zip_file.infolist()) == 1:
        info = zip_file.infolist()[0]
        info.filename = dest_prefix.with_suffix(PurePath(info.filename).suffix).name
        zip_file.extract(info, path=dest_prefix.parent)
    else:
        dest_prefix.mkdir(parents=True, exist_ok=True)
        zip_file.extractall(dest_prefix)


def get_archive_targets(archive_path, dest_prefix: Path):
    if archive_path.suffix == ".zip":
        return get_zip_targets(archive_path, dest_prefix)

    with archive_reader(str(archive_path)) as r:
        paths = [entry.pathname for entry in r if entry.filetype.IFREG]

        if len(paths) == 1:
            return [dest_prefix.with_suffix(PurePath(paths[0]).suffix)]
        else:
            return [dest_prefix / path for path in paths]


def extract_archive(archive_path, dest_prefix: Path):
    if archive_path.suffix == ".zip":
        return extract_zip_file(archive_path, dest_prefix)

    singlefile = len(get_archive_targets(archive_path, dest_prefix)) == 1

    with archive_reader(str(archive_path)) as r:
        for entry in r:
            if entry.filetype.IFREG:
                if singlefile:
                    dest = dest_prefix.with_suffix(PurePath(entry.pathname).suffix)
                else:
                    dest = dest_prefix / entry.pathname

                dest.parent.mkdir(parents=True, exist_ok=True)

                with dest.open("wb") as f:
                    for block in entry.get_blocks():
                        f.write(block)


def remove_last(it, n=1):
    try:
        value = deque((next(it) for _ in range(n)), maxlen=n)
    except StopIteration:
        return

    for n in it:
        yield value[0]
        value.append(n)


def camelcase_to_snakecase(s):
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower()


LETTER_RE = re.compile("\W+")
SPACE_RE = re.compile("\s+")


def normaliser_nom(s):
    return (
        SPACE_RE.sub(
            " ",
            LETTER_RE.sub(
                " ",
                unicodedata.normalize("NFKD", s)
                .encode("ascii", errors="ignore")
                .decode("ascii"),
            ),
        )
        .strip()
        .lower()
    )


def normaliser_colonne(s: pd.Series):
    return (
        s.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .str.lower()
        .str.strip()
        .str.replace("\s*-\s*", " ", regex=True)
    )
