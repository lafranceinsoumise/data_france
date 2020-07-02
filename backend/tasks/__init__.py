from doit import create_after
from doit.tools import create_folder

from backend import SOURCE_DIR, PREPARE_DIR
from sources import iterate_sources
from utils import (
    check_hash,
    get_archive_targets,
    extract_archive,
)
from .cog import *
from .elections import *
from .admin_express import *
from .final_data import *


def task_telecharger():
    for source in iterate_sources():
        yield {
            "name": str(source.path),
            "targets": [SOURCE_DIR / source.filename],
            "actions": [
                (create_folder, [(SOURCE_DIR / source.filename).parent]),
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
                        "--outdir",
                        SOURCE_DIR / source.filename.parent,
                        SOURCE_DIR / source.filename,
                    ]
                ],
            }


@create_after(executed="telecharger")
def task_decompresser():
    for source in iterate_sources():
        if source.suffix in [".zip", ".001"]:
            archive_path = SOURCE_DIR / source.filename
            dest_prefix = PREPARE_DIR / source.path
            targets = get_archive_targets(archive_path, dest_prefix)

            yield {
                "name": source.path,
                "file_dep": [archive_path],
                "targets": targets,
                "actions": [(extract_archive, [archive_path, dest_prefix])],
            }
