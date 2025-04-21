from dataclasses import dataclass
from .ColCleaner import ColCleaner


@dataclass
class NoneCleaner(ColCleaner):
    def _get_cleaner(self):
        return lambda value: value
