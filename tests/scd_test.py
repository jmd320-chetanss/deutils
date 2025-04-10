# Databricks notebook source
# MAGIC %md
# MAGIC # Upsert Tests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %run "../utils/scd2"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Check

# COMMAND ----------

from pyspark.sql.functions import lit


def do_test(df: DataFrame, expected: DataFrame) -> None:
    # Not comparing _effective_from and _effective_to because we cannot know the timestamps before hand
    columns_to_compare = [c for c in df.columns if c not in ["_effective_from", "_effective_to"]]

    df_filtered = df.select(columns_to_compare)
    expected_filtered = expected.select(columns_to_compare)

    # Find the differences
    differences = df_filtered.exceptAll(expected_filtered).union(expected_filtered.exceptAll(df_filtered))

    timestamp_mismatches = df.join(expected, columns_to_compare, "inner").filter(
        ((expected["_effective_from"] == lit("VALUE")) & df["_effective_from"].isNull()) |
        ((expected["_effective_to"] == lit("VALUE")) & df["_effective_to"].isNull()) |
        ((expected["_effective_to"].isNull()) & df["_effective_to"].isNotNull())
    )

    if differences.count() == 0 and timestamp_mismatches.count() == 0:
        log_success("Test Passed: DataFrames match.")
    else:
        log_error("Test Failed: Differences found.")
        if differences.count() > 0:
            log_error("Column value mismatches:")
            differences.show()
        if timestamp_mismatches.count() > 0:
            log_error("Timestamp mismatches:")
            timestamp_mismatches.show()

log_level = LogLevel.Debug

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Case 0

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, TimestampType

# Test data
source_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob", "age": 30},
    {"id": 3, "name": "Charlie", "age": 35},
])

log_info("Source DataFrame: ")
source_df.display()

# Generate scd target dataframe
target_df = generate_scd2_from_df_to_df(
    source_df=source_df,
    target_df=None,
    key_cols=["id"]
)

log_info("Target DataFrame: ")
target_df.display()

# Perform test
expected_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("_effective_from", StringType(), nullable=True),
    StructField("_effective_to", StringType(), nullable=True),
    StructField("_reason", StringType(), nullable=True),
    StructField("_active", BooleanType(), nullable=True)
])

expected_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 2, "name": "Bob", "age": 30, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 3, "name": "Charlie", "age": 35, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True}
], schema=expected_schema)

log_info("Expected DataFrame: ")
expected_df.display()

do_test(target_df, expected_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Case 1

# COMMAND ----------

# Test data
source_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob 1", "age": 30}, # Modified record
    # Deleted Charlie
    {"id": 4, "name": "Chetan", "age": 24}, # New record
    {"id": 5, "name": "Meenachi", "age": 24}, # New record
])

log_info("Source DataFrame: ")
source_df.display()

log_info("Target DataFrame: ")
target_df.display()

# Generate scd target dataframe
target_df = generate_scd2_from_df_to_df(
    source_df=source_df,
    target_df=target_df,
    key_cols=["id"]
)

log_info("Target DataFrame: ")
target_df.display()

# Perform test
expected_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 4, "name": "Chetan", "age": 24, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 5, "name": "Meenachi", "age": 24, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 3, "name": "Charlie", "age": 35, "_effective_from": "VALUE", "_effective_to": "VALUE", "_reason": "Delete", "_active": False},
    {"id": 2, "name": "Bob", "age": 30, "_effective_from": "VALUE", "_effective_to": "VALUE", "_reason": "Update", "_active": False},
    {"id": 2, "name": "Bob 1", "age": 30, "_effective_from": "VALUE", "_effective_to": None, "_reason": "Update", "_active": True}
], schema=expected_schema)

log_info("Expected DataFrame: ")
expected_df.display()

do_test(target_df, expected_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Case 2

# COMMAND ----------

# Test data
source_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25},
    {"id": 2, "name": "Bob 2", "age": 30}, # Modified record
    {"id": 4, "name": "Chetan", "age": 24},
    {"id": 5, "name": "Meenachi", "age": 24},
])

log_info("Source DataFrame: ")
source_df.display()

log_info("Target DataFrame: ")
target_df.display()

# Generate scd target dataframe
target_df = generate_scd2_from_df_to_df(
    source_df=source_df,
    target_df=target_df,
    key_cols=["id"],
)

log_info("Target DataFrame: ")
target_df.display()

# Perform test
expected_df = spark.createDataFrame([
    {"id": 1, "name": "Alice", "age": 25, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 4, "name": "Chetan", "age": 24, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 5, "name": "Meenachi", "age": 24, "_effective_from": "VALUE", "_effective_to": None, "_reason": "New", "_active": True},
    {"id": 3, "name": "Charlie", "age": 35, "_effective_from": "VALUE", "_effective_to": "VALUE", "_reason": "Delete", "_active": False},
    {"id": 2, "name": "Bob", "age": 30, "_effective_from": "VALUE", "_effective_to": "VALUE", "_reason": "Update", "_active": False},
    {"id": 2, "name": "Bob 1", "age": 30, "_effective_from": "VALUE", "_effective_to": "VALUE", "_reason": "Update", "_active": False},
    {"id": 2, "name": "Bob 2", "age": 30, "_effective_from": "VALUE", "_effective_to": None, "_reason": "Update", "_active": True}
], schema=expected_schema)

log_info("Expected DataFrame: ")
expected_df.display()

do_test(target_df, expected_df)
