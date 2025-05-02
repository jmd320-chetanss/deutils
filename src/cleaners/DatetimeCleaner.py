from dataclasses import dataclass
from .ColCleaner import ColCleaner
from datetime import date
from dateutil import parser


@dataclass
class DatetimeCleaner(ColCleaner):
    """
    Class to clean datetime columns in a DataFrame.
    """

    # Parse formats of the datetime
    parse_formats: str | list[str] = "%Y-%m-%d %H:%M:%S"

    # Format of the datetime
    format: str = "%Y-%m-%d %H:%M:%S"

    def __post_init__(self):
        self.datatype = f"timestamp"

    def _get_cleaner(self):
        """
        Clean the datetime value by converting to datetime.
        """

        parse_formats = self.parse_formats
        if parse_formats is None:
            pass
        elif isinstance(parse_formats, str):
            parse_formats = [parse_formats]

        def cleaner(value: str | None) -> str | None:
            if value is not None and value.strip() == "" and self.empty_to_null:
                value = None

            if value is None:
                if self.nullable:
                    return None
                else:
                    raise ValueError("Value cannot be null")

            try:
                parsed_value = parser.parse(value)
            except (ValueError, TypeError):
                raise ValueError(f"Cannot parse {type(value)} '{value}' as date")

            return parsed_value.strftime(self.format)

        return cleaner
