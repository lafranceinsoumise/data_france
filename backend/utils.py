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


def extract_singlefile(archive_path, dest, expected_ext):
    with archive_reader(str(archive_path)) as r:
        entry = next(r)

        ext = PurePath(entry.pathname).suffix

        if ext != f".{expected_ext}":
            raise ValueError(
                f"Le fichier dans l'archive {archive_path} n'a pas l'extension attendue"
            )

        with dest.open("wb") as f:
            for block in entry.get_blocks():
                f.write(block)

        try:
            entry = next(r)
        except StopIteration:
            pass
        else:
            raise ValueError(f"L'archive {archive_path} contient plus d'un fichier !")


def extract_archive(archive_path, dest_prefix: Path, targets):
    with archive_reader(str(archive_path)) as r:
        for entry in r:
            if entry.filetype.IFREG:
                name = PurePath(entry.pathname).name
                if name not in targets:
                    continue
                dest = dest_prefix / name

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
