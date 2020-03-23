import hashlib
from pathlib import Path, PurePath
from zipfile import ZipFile

BLOCKSIZE = 65536


class check_hash:
    def __init__(self, filename, hash_digest):
        self.filename = filename
        self.hash_digest = hash_digest

    def _check(self):
        hasher = hashlib.new("sha256")

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


def ensure_dir_exists(filename: Path):
    return ["mkdir", "-p", filename.parent]


def get_zip_targets(zip_path, dest_prefix):
    zip_file = ZipFile(zip_path)
    if len(zip_file.infolist()) == 1:
        return [
            dest_prefix.with_suffix(PurePath(zip_file.infolist()[0].filename).suffix)
        ]

    else:
        return [dest_prefix / info.filename for info in zip_file.infolist()]


def extract_zip_file(zip_path, dest_prefix: Path):
    zip_file = ZipFile(zip_path)
    if len(zip_file.infolist()) == 1:
        info = zip_file.infolist()[0]
        info.filename = dest_prefix.with_suffix(PurePath(info.filename).suffix).name
        zip_file.extract(info, path=dest_prefix.parent)
    else:
        dest_prefix.mkdir(parents=True, exist_ok=True)
        zip_file.extractall(dest_prefix)
