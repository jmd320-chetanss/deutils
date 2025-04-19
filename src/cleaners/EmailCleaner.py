from dataclasses import dataclass
from . import ColCleaner


@dataclass
class EmailCleaner(ColCleaner):

    def get_udf(self):
        pass
