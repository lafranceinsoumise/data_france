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
from .assemblee_nationale import *

# Patch RGB color because openpyxl does not support old XLS file with color
from openpyxl.styles.colors import WHITE, RGB
__old_rgb_set__ = RGB.__set__

def __rgb_set_fixed__(self, instance, value):
    try:
        __old_rgb_set__(self, instance, value)
    except ValueError as e:
        if e.args[0] == 'Colors must be aRGB hex values':
            __old_rgb_set__(self, instance, WHITE)  # Change color here

RGB.__set__ = __rgb_set_fixed__


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
                    "--location",
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
                    extract_singlefile(archive_path, targets[0], source.extraire),
                ]
            else:
                targets = [dest_prefix / p for p in source.extraire]
                actions = [
                    (create_folder, (dest_prefix,)),
                    extract_archive(archive_path, dest_prefix, source.extraire),
                ]
            yield {
                "name": source.path,
                "file_dep": [archive_path],
                "targets": targets,
                "actions": actions,
            }
