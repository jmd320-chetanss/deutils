# Databricks notebook source
# MAGIC %md
# MAGIC ## Cleaning Tests

# COMMAND ----------

# MAGIC %run "../utils/cleaning"

# COMMAND ----------

source_df = spark.createDataFrame([
    { "name": "Alice", "age": "25", "salary": "100000",  "dob": "2022-01-01", "updated_at": "2022-01-01 12:00:00", "avg_workhours": "8.8888" },
    { "name": "  Chetan  ", "age": "  25  ", "salary": "  100000  ",  "dob": "  2022-01-01  ", "updated_at": "  2022-01-01 12:00:00  ", "avg_workhours": "  8.5  " }, # Checking space handling
    { "name": "Alice", "age": "25", "salary": "  23242.223233  ",  "dob": "01-01-2022", "updated_at": "2022-01-01 12:00:00", "avg_workhours": "8.5234" },
    { "name": "Alice", "age": "25", "salary": "  23242.223233  ",  "dob": "01-01-2022", "updated_at": "2022-01-01 12:00:00", "avg_workhours": "8.5234" },
])

target_df = clean_table_df_to_df(
    df=source_df,
    schema={
        "age": SchemaType.Unsigned(),
        "salary": SchemaType.Currency(),
        "dob": SchemaType.Date(),
        "updated_at": SchemaType.Datetime(),
        "avg_workhours": SchemaType.Decimal(),
    }
)

print(target_df.dtypes)
target_df.show()
