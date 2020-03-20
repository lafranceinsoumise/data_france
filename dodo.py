import sys
from importlib import import_module
from pathlib import Path, PurePath

import yaml

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
            ],
            "uptodate": [check_hash(SOURCE_DIR / source.filename, source.hash)],
        }


def task_convertir_en_csv():
    for source in iterate_sources():
        if source.to_csv:
            yield {
                "name": str(source.path),
                "file_dep": [SOURCE_DIR / source.filename],
                "targets": [(SOURCE_DIR / source.filename).with_suffix(".csv")],
                "actions": [
                    [
                        "libreoffice",
                        "--headless",
                        "--convert-to",
                        "csv",
                        SOURCE_DIR / source.filename,
                    ]
                ],
            }


def task_preparer():
    for source in iterate_sources():
        if source.preparation is not None:
            module, func = source.preparation.split(":", 1)
            module = import_module(module)
            func = getattr(module, func)

            src = SOURCE_DIR / source.filename
            if source.to_csv:
                src = src.with_suffix(".csv")

            base_filenames = [PREPARE_DIR / t for t in source.targets]

            targets = [
                f.with_name(f.name + ext)
                for f in base_filenames
                for ext in ["-pop.feather", "-votes.feather"]
            ]

            print(f"{base_filenames!r}  / {targets!r}")

            yield {
                "name": source.path,
                "targets": targets,
                "file_dep": [src],
                "actions": [
                    *[ensure_dir_exists(t) for t in base_filenames],
                    (
                        func,
                        [],
                        {
                            "src": src,
                            "base_filenames": base_filenames,
                            "delimiter": "," if source.to_csv else ";",
                        },
                    ),
                ],
            }
