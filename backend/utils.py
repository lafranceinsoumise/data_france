import hashlib
from pathlib import Path

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
