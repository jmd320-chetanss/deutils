from dataclasses import dataclass
from typing import Literal
import pyspark.sql.functions as spf
from . import ColCleaner


@dataclass
class BoolCleaner(ColCleaner):
    # How to represent boolean values
    mode: Literal["truefalse", "tf", "yesno", "yn", "01"] = "truefalse"

    # Should the output format be in lowercase
    lower: bool = False

    def get_udf(self):
        cleaner_udf = spf.udf(self._clean, returnType="boolean")
        return cleaner_udf
        # return lambda col: cleaner_udf(col).cast("boolean")

    def _clean(self, value: str | None):
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
