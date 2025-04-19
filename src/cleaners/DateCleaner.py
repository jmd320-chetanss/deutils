from dataclasses import dataclass
from typing import List
from datetime import datetime
import pyspark.sql.functions as spf
from . import ColCleaner


@dataclass
class DatetimeCleaner(ColCleaner):

    # Datetime formats to parse the date, each format is tried until success
    parse_fmt: str | List[str] = None

    # Output datetime format
    fmt: str = None

    def get_udf(self) -> callable:
        """
        Returns a UDF that cleans datetime values.
        """

        cleaner_udf = spf.udf(self._clean, returnType="date")
        return cleaner_udf

    def _clean(self, value: str | None) -> datetime | None:
        if value is not None and value.strip() == "" and self.empty_to_null:
            value = None

        if value is None:
            if self.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        parse_fmt = self.parse_fmt
        if parse_fmt is None:
            pass
        elif isinstance(parse_fmt, str):
            parse_fmt = [parse_fmt]

        try:
            parsed_value = parser.parse(value)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot parse {type(value)} '{value}' as date")

        return parsed_value
