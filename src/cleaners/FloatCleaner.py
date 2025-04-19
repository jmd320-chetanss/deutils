from dataclasses import dataclass
from . import ColCleaner


@dataclass
class FloatCleaner(ColCleaner):
    # Precision of the decimal
    precision: int = 3


def _get_decimal_cleaner(schema_type: SchemaType.Decimal) -> callable:
    @spf.udf()
    def cleaner_udf(value: str | None):
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        parsed_value = math_utils.parse_float(value)
        if parsed_value is None:
            raise ValueError(f"Cannot parse '{value}' as decimal")

        parsed_value = math_utils.floor_float(parsed_value, schema_type.precision)
        return parsed_value

    return lambda col: cleaner_udf(col).cast(f"decimal(38, {schema_type.precision})")
