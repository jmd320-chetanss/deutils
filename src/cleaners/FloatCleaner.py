from dataclasses import dataclass
from .ColCleaner import ColCleaner
from .. import math_utils


@dataclass
class FloatCleaner(ColCleaner):
    """
    Class to clean float columns in a DataFrame.
    """

    # Precision of the decimal
    precision: int = 2

    def __post_init__(self):
        self.datatype = f"decimal(38, {self.precision})"

    def _get_cleaner(self):
        """
        Clean the float value by removing non-numeric characters and converting to float.
        """

        def cleaner(value: str | None):
            if value is not None and value.strip() == "" and self.empty_to_null:
                value = None

            if value is None:
                if self.nullable:
                    return None
                else:
                    raise ValueError("Value cannot be null")

            parsed_value = math_utils.parse_float(value)
            if parsed_value is None:
                raise ValueError(f"Cannot parse '{value}' as decimal")

            parsed_value = math_utils.floor_float(parsed_value, self.precision)
            return parsed_value

        return cleaner
