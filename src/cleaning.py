from typing import Union, List, Optional, Literal
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
import pyspark.sql.functions as spf
from dateutil import parser
from datetime import date, datetime
from dataclasses import dataclass, field
import wordninja

from . import logs
from . import string_utils
from . import math_utils
from . import misc_utils


@dataclass
class Options:
    decimal_precision = 2
    currency_precision = 3
    date_in_fmts = ["%Y-%m-%d", "%d-%m-%Y"]
    datetime_in_fmts = ["%Y-%m-%d %H:%M:%S"]
    date_fmt = "%Y-%m-%d"
    datetime_fmt = "%Y-%m-%d %H:%M:%S"


class SchemaType:
    @dataclass
    class _Base:
        # Can the column contain null values
        nullable: bool = True

        # Every value must be unique
        unique: bool = False

        # Convert empty values to null
        empty_to_null: bool = True

        # What to replace null values with
        # NOTE: this value is then parsed by the logic as they were already there
        on_null: Optional[str] = None

        # Rename the column
        rename_to: Optional[str] = None

        # Is this a key column
        key: bool = False

    @dataclass
    class Null(_Base):
        pass

    @dataclass
    class Auto(_Base):
        pass

    @dataclass
    class String(_Base):
        # Minimum length of the string
        min_length: int = 0

        # Maximum length of the string
        max_length: int = 255

        # Should the string be trimmed
        trim: bool = True

        # Should the string be converted to lowercase
        case: Literal["lower", "upper", "snake", "camel", "pascal"] | None = None

    @dataclass
    class Bool(_Base):
        # How to represent boolean values
        mode: Literal["truefalse", "tf", "yesno", "yn", "01"] = "truefalse"

        # Should the output format be in lowercase
        lower: bool = False

    @dataclass
    class Signed(_Base):
        # Minimum value of the signed integer
        min_value: Optional[int] = None

        # Maximum value of the signed integer
        max_value: Optional[int] = 9223372036854775807

    @dataclass
    class Unsigned(_Base):
        # Minimum value of the unsigned integer
        min_value: int = 0

        # Maximum value of the unsigned integer
        max_value: Optional[int] = 9223372036854775807

    @dataclass
    class Decimal(_Base):
        # Precision of the decimal
        precision: int = Options.decimal_precision

    @dataclass
    class Currency(_Base):
        # Precision of the currency
        precision: int = Options.currency_precision

    @dataclass
    class Date(_Base):
        # Date formats to parse the date, each format is tried until success
        parse_fmt: str | List[str] = field(default_factory=lambda: Options.date_in_fmts)

        # Output date format
        fmt: str = Options.date_fmt

    @dataclass
    class Datetime(_Base):
        # Datetime formats to parse the date, each format is tried until success
        parse_fmt: str | List[str] = field(
            default_factory=lambda: Options.datetime_in_fmts
        )

        # Output datetime format
        fmt: str = Options.datetime_fmt

    @dataclass
    class Gender(_Base):
        # Output format to represent gender
        fmt: Literal["malefemale", "mf"] = "malefemale"

        # Should the output format be in lowercase
        lower: bool = False

    @dataclass
    class Phone(_Base):
        # The separator between pairs in the number
        separator: Literal["-", " ", ""] = "-"

        # Should the output has country code
        include_country_code: bool = True

    @dataclass
    class Email(_Base):
        pass

    @dataclass
    class Enum(String):
        # The possible values for the enum
        values: List[str] = field(default_factory=list)


# For easy access using python modules
Auto = SchemaType.Auto
Null = SchemaType.Null
String = SchemaType.String
Bool = SchemaType.Bool
Signed = SchemaType.Signed
Unsigned = SchemaType.Unsigned
Decimal = SchemaType.Decimal
Currency = SchemaType.Currency
Date = SchemaType.Date
Datetime = SchemaType.Datetime
Gender = SchemaType.Gender

SchemaTypeUnion = Union[
    SchemaType.Auto,
    SchemaType.String,
    SchemaType.Signed,
    SchemaType.Unsigned,
    SchemaType.Decimal,
    SchemaType.Currency,
    SchemaType.Date,
    SchemaType.Datetime,
    SchemaType.Gender,
]


def get_key_columns(schema: dict[str, SchemaTypeUnion]) -> List[str]:
    return [col_name for col_name, schema_type in schema.items() if schema_type.key]


def get_unique_columns(schema: dict[str, SchemaTypeUnion]) -> List[str]:
    return [col_name for col_name, schema_type in schema.items() if schema_type.unique]


def _get_null_cleaner(schema_type: SchemaType.Null) -> callable:
    return lambda col: spf.col(col).cast("string")

def _get_case_updater(case: str) -> callable:
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


def _get_string_cleaner(schema_type: SchemaType.String) -> callable:
    case_updater = _get_case_updater(schema_type.case)

    if case_updater is None:
        raise ValueError(f"Invalid case '{schema_type.case}'.")

    @spf.udf(returnType="string")
    def cleaner_udf(value: str | None):
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        if schema_type.trim:
            value = value.strip()

        value = case_updater(value)
        return value

    return lambda col: cleaner_udf(col).cast("string")


def _get_auto_cleaner(schema_type: SchemaType.String) -> callable:
    return _get_string_cleaner(SchemaType.String())


def _get_bool_cleaner(schema_type: SchemaType.Bool) -> callable:
    @spf.udf(returnType="boolean")
    def cleaner_udf(value: str | None):
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        value_clean = value.lower().strip()
        istrue = value_clean in ["true", "t", "yes", "y", "on", "1"]
        isfalse = value_clean in ["false", "f", "no", "n", "off", "0"]

        if not istrue and not isfalse:
            raise ValueError(f"Cannot parse '{value}' as boolean")

        if schema_type.mode == "truefalse":
            result = "True" if istrue else "False"
            return result.tolower() if schema_type.lower else result

        if schema_type.mode == "tf":
            result = "T" if istrue else "F"
            return result.tolower() if schema_type.lower else result

        if schema_type.mode == "yesno":
            result = "Yes" if istrue else "No"
            return result.tolower() if schema_type.lower else result

        if schema_type.mode == "yn":
            result = "Y" if istrue else "N"
            return result.tolower() if schema_type.lower else result

        if schema_type.mode == "01":
            return "1" if istrue else "0"

        raise ValueError(f"Invalid mode '{schema_type.mode}'")

    return lambda col: cleaner_udf(col).cast("boolean")


def _get_int_cleaner(
    schema_type: SchemaType.Signed | SchemaType.Unsigned,
) -> callable:
    integer_type = "signed" if schema_type == SchemaType.Signed else "unsigned"

    @spf.udf(returnType="bigint")
    def cleaner_udf(value: str | None):
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None

            raise ValueError("Value cannot be null")

        parsed_value = math_utils.parse_int(value)

        if parsed_value is None:
            raise ValueError(f"Cannot parse '{value}' as {integer_type} integer.")

        if schema_type.min_value is not None and parsed_value < schema_type.min_value:
            raise ValueError(
                f"Value '{value}' parsed as '{parsed_value}' is less than the specified '{schema_type.min_value}' value."
            )

        if schema_type.max_value is not None and parsed_value > schema_type.max_value:
            raise ValueError(
                f"Value '{value}' parsed as '{parsed_value}' is greater than the specified '{schema_type.max_value}' value."
            )

        return parsed_value

    return lambda col: cleaner_udf(col).cast("bigint")


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


def _get_date_cleaner(schema_type: SchemaType.Date) -> callable:
    @spf.udf(returnType="date")
    def cleaner_udf(value: str | None) -> date | None:
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        parse_fmt = schema_type.parse_fmt
        if parse_fmt is None:
            pass
        elif isinstance(parse_fmt, str):
            parse_fmt = [parse_fmt]

        try:
            parsed_value = parser.parse(value)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot parse {type(value)} '{value}' as date")

        return parsed_value

    return lambda col: cleaner_udf(col)


def _get_datetime_cleaner(schema_type: SchemaType.Datetime) -> callable:
    @spf.udf(returnType="timestamp")
    def cleaner_udf(value: str | None) -> datetime | None:
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        try:
            parsed_value = parser.parse(value)
        except (ValueError, TypeError):
            raise ValueError(f"Cannot parse '{value}' as datetime")

        return parsed_value

    return lambda col: cleaner_udf(col).cast("timestamp")


def _get_gender_cleaner(schema_type: SchemaType.Datetime) -> callable:
    @spf.udf(returnType="string")
    def cleaner_udf(value: str | None):
        if value is not None and value.strip() == "" and schema_type.empty_to_null:
            value = None

        if value is None:
            if schema_type.nullable:
                return None
            else:
                raise ValueError("Value cannot be null")

        value_clean = value.lower().strip()
        ismale = value_clean in ["male", "m"]
        isfemale = value_clean in ["female", "f"]

        if not ismale and not isfemale:
            raise ValueError(f"Cannot parse '{value}' as gender")

        if schema_type.fmt == "malefemale":
            result = "Male" if ismale else "Female"
            return result.lower() if schema_type.lower else result

        if schema_type.fmt == "mf":
            result = "M" if ismale else "F"
            return result.lower() if schema_type.lower else result

        raise ValueError(f"Invalid output format '{schema_type.fmt}'")

    return lambda col: cleaner_udf(col).cast("string")


def _get_phone_cleaner(schema_type: SchemaType.Datetime) -> callable:
    return _get_string_cleaner(SchemaType.String())


def _get_email_cleaner(schema_type: SchemaType.Email) -> callable:
    return _get_string_cleaner(SchemaType.String())


def _get_enum_cleaner(schema_type: SchemaType.Enum) -> callable:
    return _get_string_cleaner(SchemaType.String())


def _get_col_cleaner(schema_type: SchemaTypeUnion) -> callable:

    if isinstance(schema_type, SchemaType.Null):
        return _get_null_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Auto):
        return _get_auto_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.String):
        return _get_string_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Bool):
        return _get_bool_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Signed):
        return _get_int_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Unsigned):
        return _get_int_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Decimal):
        return _get_decimal_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Currency):
        return _get_decimal_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Date):
        return _get_date_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Datetime):
        return _get_datetime_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Gender):
        return _get_gender_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Phone):
        return _get_phone_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Email):
        return _get_email_cleaner(schema_type)

    if isinstance(schema_type, SchemaType.Enum):
        return _get_enum_cleaner(schema_type)

    return None


class Result:
    value: DataFrame | ConnectDataFrame
    renamed_cols: dict[str, str]

    def __init__(
        self,
        value: DataFrame | ConnectDataFrame,
        renamed_cols: dict[str, str],
    ):

        assert isinstance(value, (DataFrame, ConnectDataFrame))
        assert isinstance(renamed_cols, dict)

        self.value = value
        self.renamed_cols = renamed_cols


def clean_table(
    df: DataFrame | ConnectDataFrame,
    schema: dict[str, SchemaTypeUnion],
) -> Result:
    """
    Handles the following tasks:
    - Trims all string columns
    - Handles decimal precision
    - Handles currency precision
    - Handles date fmt
    - Handles datetime fmt
    - Consistent column names
    """

    # Convert every column to string
    for col in df.columns:
        df = df.withColumn(col, spf.col(col).cast("string"))

    default_data_type = schema.get("*", SchemaType.Auto())

    # -----------------------------------------------------------------------------------------------------
    # Cleaning columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Cleaning columns...")

    for col in df.columns:
        schema_type = schema.get(col, default_data_type)
        logs.log_debug(
            f"Setting data type of '{col}' to '{misc_utils.get_type_name(schema_type)}'..."
        )

        cleaner = _get_col_cleaner(schema_type)
        if cleaner is None:
            logs.log_warn(
                f"Setting '{col}' to '{misc_utils.get_type_name(schema_type)}' failed, unknown data type."
            )

            continue

        df = df.withColumn(col, cleaner(col))

    logs.log_success("Cleaning columns done.")

    # -----------------------------------------------------------------------------------------------------
    # Checking for unique columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Checking for unique columns...")

    unique_cols = get_unique_columns(schema)
    logs.log_debug(f"Checking in {unique_cols}.")

    for col in unique_cols:
        unique_count = df.select(col).distinct().count()
        total_count = df.count()
        if unique_count != total_count:
            raise ValueError(
                f"Column '{col}' is supposed to be unique but has duplicate values."
            )

    logs.log_success("Checking for unique columns done.")

    # -----------------------------------------------------------------------------------------------------
    # Checking for key columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Checking for key columns...")

    key_columns = get_key_columns(schema)
    if key_columns:
        duplicates_df = df.groupBy(key_columns).count().filter(spf.col("count") > 1)
        are_unique = duplicates_df.isEmpty()

        if not are_unique:
            if len(key_columns) > 1:
                raise ValueError(
                    f"Composite key columns {key_columns} have duplicate values."
                )
            else:
                raise ValueError(
                    f"Primary key column {key_columns} have duplicate values."
                )

    logs.log_success("Checking for key columns done.")

    # -----------------------------------------------------------------------------------------------------
    # Renaming columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Renaming columns...")

    rename_mapping: dict[str, str] = {}

    for col in df.columns:

        # Calculate new name for the column
        schema_type = schema.get(col, default_data_type)
        if schema_type is not None and schema_type.rename_to is not None:
            new_name = schema_type.rename_to
        else:
            words = wordninja.split(col)
            new_name = "_".join(words)
            new_name = string_utils.to_snake_case(new_name)

        # Register new name for renaming only if it is different than what it already is,
        # no need to clutter up the rename mapping and logs
        if new_name != col:
            logs.log_info(f"Renaming column '{col}' to '{new_name}'...")
            rename_mapping[col] = new_name

    df = df.withColumnsRenamed(rename_mapping)

    logs.log_success(f"Renaming columns done.")

    return Result(
        value=df,
        renamed_cols=rename_mapping,
    )
