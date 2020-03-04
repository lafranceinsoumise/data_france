import sys
from pathlib import Path, PurePath

import yaml
from doit.tools import config_changed

BASE_PATH = Path(__file__).parent

sys.path.insert(0, str(BASE_PATH / "backend"))
from sources import iterate_sources as _iterate_sources
from utils import check_hash

with open(Path(__file__).parent / "sources.yml") as f:
    _sources = yaml.load(f, yaml.BaseLoader)


def iterate_sources():
    return _iterate_sources(_sources)


def source(path):
    current = _sources
    for part in PurePath(path).parts:
        current = current[part]
    return current


def task_telecharger():
    DL_DIR = BASE_PATH / "build" / "sources"
    for source in iterate_sources():
        yield {
            "name": str(source.path),
            "targets": [DL_DIR / source.filename],
            "actions": [
                ["mkdir", "-p", (DL_DIR / source.filename).parent],
                ["curl", "--silent", "--output", DL_DIR / source.filename, source.url],
                (
                    check_hash,
                    [],
                    {"filename": DL_DIR / source.filename, "hash_digest": source.hash},
                ),
            ],
            "uptodate": [config_changed(source.hash)],
        }
