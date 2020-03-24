from pathlib import Path
from flit_core.buildapi import (
    get_requires_for_build_wheel,
    get_requires_for_build_sdist,
    prepare_metadata_for_build_wheel,
    build_sdist,
    build_wheel,
)

BASE_DIR = Path(__file__).parent.parent
SOURCE_DIR = BASE_DIR / "build" / "sources"
PREPARE_DIR = BASE_DIR / "build" / "prepare"
