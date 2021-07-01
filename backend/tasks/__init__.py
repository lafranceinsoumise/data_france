from doit.tools import create_folder

from sources import SOURCES, SOURCE_DIR, PREPARE_DIR
from utils import check_hash, extract_archive, extract_singlefile
from .admin_express import *
from .cog import *
from .elections import *
from .final_data import *
from .annuaire_administratif import *
from .rne import *
from .parrainages import *
from .consulaires import *


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


def task_decompresser():
    for source in SOURCES:
        if source.suffix in [".zip", ".7z"] and source.extraire:
            archive_path = SOURCE_DIR / source.filename
            dest_prefix = PREPARE_DIR / source.path

            if isinstance(source.extraire, str):
                targets = [dest_prefix.with_suffix(f".{source.extraire}")]
                actions = [
                    (create_folder, (dest_prefix.parent,)),
                    (extract_singlefile, (archive_path, targets[0], source.extraire)),
                ]
            else:
                targets = [dest_prefix / p for p in source.extraire]
                actions = [
                    (create_folder, (dest_prefix,)),
                    (extract_archive, (archive_path, dest_prefix, source.extraire)),
                ]

            yield {
                "name": source.path,
                "file_dep": [archive_path],
                "targets": targets,
                "actions": actions,
            }
