from typing import Union, List, Optional, Literal
from pyspark.sql import DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
import pyspark.sql.functions as spf
from dateutil import parser
from datetime import date, datetime
from dataclasses import dataclass, field
from .cleaners.ColCleaner import ColCleaner
import wordninja

from . import logs
from . import string_utils


@dataclass
class Options:
    decimal_precision = 2
    currency_precision = 3
    date_in_fmts = ["%Y-%m-%d", "%d-%m-%Y"]
    datetime_in_fmts = ["%Y-%m-%d %H:%M:%S"]
    date_fmt = "%Y-%m-%d"
    datetime_fmt = "%Y-%m-%d %H:%M:%S"


def get_key_columns(schema: dict[str, ColCleaner]) -> List[str]:
    return [
        col_name
        for col_name, schema_type in schema.items()
        if not isinstance(schema_type, SchemaType.Drop) and schema_type.key
    ]


def get_unique_columns(schema: dict[str, ColCleaner]) -> List[str]:
    return [
        col_name
        for col_name, schema_type in schema.items()
        if not isinstance(schema_type, SchemaType.Drop) and schema_type.unique
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
    cleaners: dict[str, ColCleaner],
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

    default_data_type = cleaners.get("*", SchemaType.Auto())

    # -----------------------------------------------------------------------------------------------------
    # Cleaning columns
    # -----------------------------------------------------------------------------------------------------

    logs.log_info("Cleaning columns...")

    for col in df.columns:
        cleaner = cleaners.get(col, default_data_type)
        logs.log_debug(f"Cleaning '{col}' with '{cleaner.name}' cleaner...")

        if isinstance(cleaner, SchemaType.Drop):
            logs.log_info(f"Dropping column '{col}'...")
            df = df.drop(col)
            continue

        df = df.withColumn(col, cleaner(col))

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

    unique_cols = get_unique_columns(cleaners)
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

    key_cols = get_key_columns(cleaners)
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
        cleaner = cleaners.get(col, default_data_type)
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
