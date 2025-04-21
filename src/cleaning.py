from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
import pyspark.sql.functions as spf
from dataclasses import dataclass
import wordninja

from . import logs
from . import string_utils
from . import misc_utils
from .cleaners.ColCleaner import ColCleaner
from .cleaners.AutoCleaner import AutoCleaner
from .cleaners.NoneCleaner import NoneCleaner
from .cleaners.StringCleaner import StringCleaner
from .cleaners.BoolCleaner import BoolCleaner
from .cleaners.IntCleaner import IntCleaner
from .cleaners.FloatCleaner import FloatCleaner
from .cleaners.DatetimeCleaner import DatetimeCleaner
from .cleaners.GenderCleaner import GenderCleaner
from .cleaners.PhoneCleaner import PhoneCleaner
from .cleaners.EmailCleaner import EmailCleaner
from .cleaners.EnumCleaner import EnumCleaner
from .cleaners.UuidCleaner import UuidCleaner
from .cleaners.PostcodeCleaner import PostcodeCleaner
from .cleaners.DropCleaner import DropCleaner


@dataclass
class UnsignedCleaner(IntCleaner):
    """
    Note: This class is temporary and only present for backwards compatibility.
    """

    def __post_init__(self):
        super().__post_init__()
        self.min_value = 0


@dataclass
class CurrencyCleaner(FloatCleaner):
    """
    Note: This class is temporary and only present for backwards compatibility.
    """

    def __post_init__(self):
        super().__post_init__()
        self.precision = 3


@dataclass
class DateCleaner(DatetimeCleaner):
    """
    Note: This class is temporary and only present for backwards compatibility.
    """

    def __post_init__(self):
        super().__post_init__()
        self.format = "%Y-%m-%d"


# For easy access using python modules
Auto = AutoCleaner
Null = NoneCleaner
String = StringCleaner
Bool = BoolCleaner
Signed = IntCleaner
Unsigned = UnsignedCleaner
Decimal = FloatCleaner
Currency = CurrencyCleaner
Date = DateCleaner
Datetime = DatetimeCleaner
Gender = GenderCleaner
Phone = PhoneCleaner
Email = EmailCleaner
Enum = EnumCleaner
Uuid = UuidCleaner
Postcode = PostcodeCleaner
Drop = DropCleaner


def get_key_columns(schema: dict[str, ColCleaner]) -> list[str]:
    return [
        col
        for col, cleaner in schema.items()
        if not isinstance(cleaner, Drop) and cleaner.key
    ]


def get_unique_columns(schema: dict[str, ColCleaner]) -> list[str]:
    return [
        col
        for col, cleaner in schema.items()
        if not isinstance(cleaner, Drop) and cleaner.unique
    ]


class Result:
    value: DataFrame | ConnectDataFrame
    renamed_cols: dict[str, str]
    key_cols: str

    def __init__(
        self,
        value: DataFrame | ConnectDataFrame,
        renamed_cols: dict[str, str],
        key_cols: str,
    ):

        assert isinstance(value, (DataFrame, ConnectDataFrame))
        assert isinstance(renamed_cols, dict)

        self.value = value
        self.renamed_cols = renamed_cols
        self.key_cols = key_cols


def clean_table(
    df: DataFrame | ConnectDataFrame,
    schema: dict[str, ColCleaner],
    drop_complete_duplicates: bool = False,
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

    default_cleaner = schema.get("*", Auto())

    # -----------------------------------------------------------------------------------------------------
    # Cleaning columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Cleaning columns...")

    for col in df.columns:
        cleaner: ColCleaner = schema.get(col, default_cleaner)
        logs.log_debug(
            f"Cleaning col '{col}' with '{misc_utils.get_type_name(cleaner)}' cleaner..."
        )

        if isinstance(cleaner, Drop):
            logs.log_info(f"Dropping column '{col}'...")
            df = df.drop(col)
            continue

        df = df.withColumn(col, cleaner.clean_col(col))

    logs.log_success("Cleaning columns done.")

    # -----------------------------------------------------------------------------------------------------
    # Dropping complete duplicates
    # -----------------------------------------------------------------------------------------------------

    if drop_complete_duplicates:
        logs.log_info("Dropping complete duplicates...")

        before_drop_count = df.count()
        df = df.dropDuplicates()
        drop_count = before_drop_count - df.count()

        logs.log_success(f"Dropping complete duplicates done, dropped {drop_count}.")

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

    key_cols = get_key_columns(schema)
    if key_cols:
        duplicates_df = df.groupBy(key_cols).count().filter(spf.col("count") > 1)
        are_unique = duplicates_df.isEmpty()

        if not are_unique:
            if len(key_cols) > 1:
                raise ValueError(
                    f"Composite key columns {key_cols} have duplicate values."
                )
            else:
                raise ValueError(
                    f"Primary key column {key_cols} have duplicate values."
                )

    logs.log_success("Checking for key columns done.")

    # -----------------------------------------------------------------------------------------------------
    # Renaming columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Renaming columns...")

    rename_mapping: dict[str, str] = {}

    for col in df.columns:

        # Calculate new name for the column
        cleaner = schema.get(col, default_cleaner)
        if cleaner is not None and cleaner.rename_to is not None:
            new_name = cleaner.rename_to
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

    # The new names of the key columns after they are renamed
    renamed_key_cols = [rename_mapping.get(col, col) for col in key_cols]

    return Result(
        value=df,
        renamed_cols=rename_mapping,
        key_cols=renamed_key_cols,
    )
