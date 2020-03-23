import sys
from pathlib import Path

BASE_PATH = Path(__file__).parent

sys.path.insert(0, str(BASE_PATH / "backend"))
from tasks import *
