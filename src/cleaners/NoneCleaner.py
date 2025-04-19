from dataclasses import dataclass
from . import ColCleaner

@dataclass
class NoneCleaner(ColCleaner):
    pass
