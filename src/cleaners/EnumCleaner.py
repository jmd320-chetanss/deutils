from dataclasses import dataclass, field
from typing import List
from . import StringCleaner


@dataclass
class EnumCleaner(StringCleaner):
    # The possible values for the enum
    values: List[str] = field(default_factory=list)
