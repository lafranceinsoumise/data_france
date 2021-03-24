import sys
from pathlib import Path

from doit.tools import config_changed
import toml

DOIT_CONFIG = {"default_tasks": ["build"], "action_string_formatting": "both"}
BASE_PATH = Path(__file__).parent

sys.path.insert(0, str(BASE_PATH / "backend"))
from tasks import *

with (BASE_PATH / "pyproject.toml") as fd:
    pyproject = toml.load(fd)

version = pyproject["tool"]["poetry"]["version"]


def task_build():
    from tasks.final_data import __all__

    return {
        "task_dep": [t[len("task_") :] for t in __all__ if t.startswith("task_")],
        "actions": [["poetry", "build"]],
        "uptodate": [config_changed(version)],
    }
