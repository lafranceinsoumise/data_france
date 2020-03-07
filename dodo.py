import sys
from importlib import import_module
from pathlib import Path, PurePath

import yaml
from doit.tools import config_changed

BASE_PATH = Path(__file__).parent
SOURCE_DIR = BASE_PATH / "build" / "sources"
PREPARE_DIR = BASE_PATH / "build" / "prepare"

sys.path.insert(0, str(BASE_PATH / "backend"))
from sources import iterate_sources as _iterate_sources
from utils import check_hash, ensure_dir_exists

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
    for source in iterate_sources():
        yield {
            "name": str(source.path),
            "targets": [SOURCE_DIR / source.filename],
            "actions": [
                ensure_dir_exists(SOURCE_DIR / source.filename),
                [
                    "curl",
                    "--silent",
                    "--output",
                    SOURCE_DIR / source.filename,
                    source.url,
                ],
                (
                    check_hash,
                    [],
                    {
                        "filename": SOURCE_DIR / source.filename,
                        "hash_digest": source.hash,
                    },
                ),
            ],
            "uptodate": [config_changed(source.hash)],
        }


def task_preparer():
    for source in iterate_sources():
        if source.preparation is not None:
            module, func = source.preparation.split(":", 1)
            module = import_module(module)
            func = getattr(module, func)

            src = SOURCE_DIR / source.filename
            targets = [PREPARE_DIR / t for t in source.targets]

            yield {
                "name": source.path,
                "targets": targets,
                "file_dep": [src],
                "actions": [
                    *[ensure_dir_exists(t) for t in targets],
                    (func, [], {"src": src, "targets": targets}),
                ],
            }
