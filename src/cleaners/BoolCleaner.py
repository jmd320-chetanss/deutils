from dataclasses import dataclass
from .ColCleaner import ColCleaner
from typing import List


@dataclass
class BoolCleaner(ColCleaner):
    """
    A class to clean boolean values.
    """
    from dataclasses import field

    true_cases: List[str] = field(default_factory=lambda: ["true", "t", "yes", "y", "on", "1"])
    false_cases: List[str] = field(default_factory=lambda: ["false", "f", "no", "n", "off", "0"])
    extra_true_cases: List[str] = field(default_factory=list)
    extra_false_cases: List[str] = field(default_factory=list)
    true_value: str = "True"
    false_value: str = "False"

    def __post_init__(self):
        self.datatype = "boolean"

    def _get_cleaner(self) -> callable:
        """
        Cleans the input string and converts it to a boolean value.
        """

        true_cases = self.true_cases + self.extra_true_cases
        false_cases = self.false_cases + self.extra_false_cases

        def cleaner(value: str | None) -> str | None:
            if value is not None and value.strip() == "" and self.empty_to_null:
                value = None

            if value is None:
                if self.nullable:
                    return None
                else:
                    raise ValueError("Value cannot be null")

            value_clean = value.lower().strip()
            istrue = value_clean in true_cases
            isfalse = value_clean in false_cases

            if not istrue and not isfalse:
                raise ValueError(f"Cannot parse '{value}' as boolean.")

            return self.true_value if istrue else self.false_value

        return cleaner
