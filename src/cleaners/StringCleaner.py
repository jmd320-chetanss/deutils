from dataclasses import dataclass
from .ColCleaner import ColCleaner
from typing import Literal
from .. import string_utils


@dataclass
class StringCleaner(ColCleaner):
    # Minimum length of the string
    min_length: int = 0

    # Maximum length of the string
    max_length: int = 255

    # Should the string be trimmed
    trim: bool = True

    # Should the string be converted to lowercase
    case: Literal["lower", "upper", "snake", "camel", "pascal"] | None = None

    def _get_cleaner(self) -> callable:
        case_updater = self._get_case_updater(self.case)

        if case_updater is None:
            raise ValueError(f"Invalid case '{self.case}'.")

        def cleaner(value: str | None):
            if value is not None and value.strip() == "" and self.empty_to_null:
                value = None

            if value is None:
                if self.nullable:
                    return None
                else:
                    raise ValueError("Value cannot be null")

            if self.trim:
                value = value.strip()

            value = case_updater(value)
            return value

        return cleaner

    def _get_case_updater(self, case: str) -> callable:
        match case:
            case None:
                return lambda value: value
            case "lower":
                return lambda value: value.lower()
            case "upper":
                return lambda value: value.upper()
            case "snake":
                return lambda value: string_utils.to_snake_case(value)
            case "camel":
                return lambda value: string_utils.to_camel_case(value)
            case "pascal":
                return lambda value: string_utils.to_pascal_case(value)
            case _:
                return None
