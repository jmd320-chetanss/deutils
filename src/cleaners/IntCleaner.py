from dataclasses import dataclass
import pyspark.sql.functions as spf
from . import ColCleaner
from .. import math_utils
from typing import Optional


@dataclass
class IntCleaner(ColCleaner):

    # Minimum value of the signed integer
    min_value: Optional[int] = None

    # Maximum value of the signed integer
    max_value: Optional[int] = 9223372036854775807

    def get_udf(self) -> callable:
        cleaner_udf = spf.udf(self._clean, returnType="bigint")
        return cleaner_udf
        # return lambda col: cleaner_udf(col).cast("bigint")

    def _clean(self, value: str | None) -> callable:
        if value is not None and value.strip() == "" and self.empty_to_null:
            value = None

        if value is None:
            if self.nullable:
                return None

            raise ValueError("Value cannot be null")

        parsed_value = math_utils.parse_int(value)

        if parsed_value is None:
            raise ValueError(f"Cannot parse '{value}' as integer.")

        if self.min_value is not None and parsed_value < self.min_value:
            raise ValueError(
                f"Value '{value}' parsed as '{parsed_value}' is less than the specified '{self.min_value}' value."
            )

        if self.max_value is not None and parsed_value > self.max_value:
            raise ValueError(
                f"Value '{value}' parsed as '{parsed_value}' is greater than the specified '{self.max_value}' value."
            )

        return parsed_value
