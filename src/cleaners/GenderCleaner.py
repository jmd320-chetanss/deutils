from dataclasses    import dataclass
from .ColCleaner import ColCleaner
from typing import Literal


@dataclass
class GenderCleaner(ColCleaner):

    # Output format to represent gender
    fmt: Literal["malefemale", "mf"] = "malefemale"

    # Should the output format be in lowercase
    lower: bool = False

    def _get_cleaner(self) -> callable:

        def cleaner(value: str | None):
            if value is not None and value.strip() == "" and self.empty_to_null:
                value = None

            if value is None:
                if self.nullable:
                    return None
                else:
                    raise ValueError("Value cannot be null")

            value_clean = value.lower().strip()
            ismale = value_clean in ["male", "m"]
            isfemale = value_clean in ["female", "f"]

            if not ismale and not isfemale:
                raise ValueError(f"Cannot parse '{value}' as gender")

            if self.fmt == "malefemale":
                result = "Male" if ismale else "Female"
                return result.lower() if self.lower else result

            if self.fmt == "mf":
                result = "M" if ismale else "F"
                return result.lower() if self.lower else result

            raise ValueError(f"Invalid output format '{self.fmt}'")

        return cleaner
