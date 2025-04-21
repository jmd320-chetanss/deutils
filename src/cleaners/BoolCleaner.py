from dataclasses import dataclass
from typing import Literal
from .ColCleaner import ColCleaner


@dataclass
class BoolCleaner(ColCleaner):
    """
    A class to clean boolean values.
    """

    # How to represent boolean values
    mode: Literal["truefalse", "tf", "yesno", "yn", "01"] = "truefalse"

    # Should the output format be in lowercase
    lower: bool = False

    def __post_init__(self):
        self.datatype = "boolean"

    def _get_cleaner(self) -> callable:
        """
        Cleans the input string and converts it to a boolean value.
        """

        def cleaner(value: str | None):
            if value is not None and value.strip() == "" and self.empty_to_null:
                value = None

            if value is None:
                if self.nullable:
                    return None
                else:
                    raise ValueError("Value cannot be null")

            value_clean = value.lower().strip()
            istrue = value_clean in ["true", "t", "yes", "y", "on", "1"]
            isfalse = value_clean in ["false", "f", "no", "n", "off", "0"]

            if not istrue and not isfalse:
                raise ValueError(f"Cannot parse '{value}' as boolean")

            if self.mode == "truefalse":
                result = "True" if istrue else "False"
                return result.tolower() if self.lower else result

            if self.mode == "tf":
                result = "T" if istrue else "F"
                return result.tolower() if self.lower else result

            if self.mode == "yesno":
                result = "Yes" if istrue else "No"
                return result.tolower() if self.lower else result

            if self.mode == "yn":
                result = "Y" if istrue else "N"
                return result.tolower() if self.lower else result

            if self.mode == "01":
                return "1" if istrue else "0"

            raise ValueError(f"Invalid mode '{self.mode}'")

        return cleaner
