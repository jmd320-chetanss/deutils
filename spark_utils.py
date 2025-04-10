from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame


def table_exists(spark_session: SparkSession, path: str) -> bool:
    """
    Check if a table exists in the Spark catalog.

    Args:
        path (str): The path of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """

    return spark_session.catalog.tableExists(path)


def read_table(
    spark_session: SparkSession, path: str, if_exists: bool = True
) -> DataFrame | ConnectDataFrame:
    """
    Read a Delta table if it exists; otherwise, return None.

    Args:
        path (str): The path of the Delta table to read.

    Returns:
        DataFrame | ConnectDataFrame: A Spark DataFrame containing the data from the Delta table if it exists, None otherwise.
    """

    if if_exists:
        if not table_exists(spark_session, path):
            return None

    return spark_session.read.table(path)


def write_table(
    spark_session: SparkSession,
    df: DataFrame | ConnectDataFrame,
    path: str,
    mode: str = "overwrite",
    format: str = "delta",
):
    """
    Write a Spark DataFrame to a Delta table at the specified path, overwriting any existing table.
    """

    df.write.saveAsTable(path, mode=mode, format=format)


def drop_table(spark_session: SparkSession, path: str, if_exists: bool = True):
    spark_session.sql(f"drop table {'if exists' if if_exists else ''} {path}")


def list_tables_from_schema(
    spark_session: SparkSession, path: str, pattern: Optional[str] = None
) -> list:
    return [
        table
        for table in spark_session.catalog.listTables(path, pattern=pattern)
        if not table.isTemporary
    ]


def list_table_paths_from_schema(
    spark_session: SparkSession, path: str, pattern: Optional[str] = None
) -> list:
    return [
        f"{path}.{table.name}"
        for table in list_tables_from_schema(spark_session, path, pattern)
    ]


def drop_tables_from_schema(
    spark_session: SparkSession, path: str, pattern: Optional[str] = None
):
    """
    Drop all the tables from the specified schema.

    Args:
        path (str): The path of the schema to drop tables from.
        pattern (Optional[str]): An optional pattern to filter the tables to drop.
    """

    table_paths = list_table_paths_from_schema(path, pattern)
    for table_path in table_paths:
        spark_session.sql(f"drop table {table_path}")


def drop_schema(
    spark_session: SparkSession, path: str, if_exists: bool = True, cascade: bool = True
):
    spark_session.sql(
        f"drop schema {'if exists' if if_exists else ''} {path} {'cascade' if cascade else ''}"
    )


def schema_exists(spark_session: SparkSession, path: str) -> bool:
    """
    Check if a schema exists in the Spark catalog.

    Args:
        path (str): The path of the schema to check.

    Returns:
        bool: True if the schema exists, False otherwise.
    """

    return spark_session.catalog.schemaExists(path)


def create_schema(spark_session: SparkSession, path: str, if_not_exists: bool = False):
    """
    Creates the specified schema.

    Args:
        path (str): The path of the schema to create.
        if_not_exists (bool): Whether to ignore the error if the schema already exists. Defaults to False.
    """

    spark_session.sql(
        f"create schema {'if not exists' if if_not_exists else ''} {path}"
    )
