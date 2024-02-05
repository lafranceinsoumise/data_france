import hashlib
import re
import unicodedata
from collections import deque
from pathlib import Path, PurePath
from zipfile import ZipFile

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

        self.computed_digest = hasher.hexdigest()
        return self.computed_digest == self.hash_digest

    def _check_after_run(self):
        if not self._check():
            raise RuntimeError(
                f"Hash incorrect pour '{self.filename}', obtenu {self.computed_digest}"
            )

        return {}

    def __call__(self, task, values):
        task.value_savers.append(self._check_after_run)
        return self._check()


def extract_singlefile(archive_path, dest, expected_ext):
    return f'[ "$(7z l -ba {archive_path} | wc -l)" -eq "1"] ; 7z e -so "{archive_path}" > {dest}'


def extract_archive(archive_path, dest_prefix: Path, targets):
    target_args = [f'"{t}"' for t in targets]
    return f"7z e '-o{dest_prefix}' '{archive_path}' {' '.join(target_args)}"


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


LETTER_RE = re.compile(r"\W+")
SPACE_RE = re.compile(r"\s+")


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
        .str.replace(r"\s*-\s*", " ", regex=True)
    )
