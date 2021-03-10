from doit import create_after
from doit.tools import create_folder

from sources import SOURCES, SOURCE_DIR, PREPARE_DIR
from utils import (
    check_hash,
    get_archive_targets,
    extract_archive,
)
from .admin_express import *
from .cog import *
from .elections import *
from .final_data import *
from .annuaire_administratif import *
from .rne import *
from .parrainages import *


def task_telecharger():
    for source in SOURCES:
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


@create_after(executed="telecharger")
def task_decompresser():
    for source in SOURCES:
        if source.suffix in [".zip", ".7z"]:
            archive_path = SOURCE_DIR / source.filename
            dest_prefix = PREPARE_DIR / source.path
            targets = get_archive_targets(archive_path, dest_prefix)

            yield {
                "name": source.path,
                "file_dep": [archive_path],
                "targets": targets,
                "actions": [(extract_archive, [archive_path, dest_prefix])],
            }
