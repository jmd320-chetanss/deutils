from dataclasses import dataclass
from typing import Literal
from . import ColCleaner

@dataclass
class PhoneCleaner(ColCleaner):
    # The separator between pairs in the number
    separator: Literal["-", " ", ""] = "-"

    # Should the output has country code
    include_country_code: bool = True
