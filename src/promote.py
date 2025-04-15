# --------------------------------------------------------------------------------------------------
# This file contains the logic to promote a table from `raw` layer to `prep` layer.
# --------------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as spf
from datetime import datetime
from . import logs
from . import cleaning
from . import spark_utils
from . import fivetran_utils
from . import scd2


def raw_to_prep(
    spark_session: SparkSession,
    source_table_path: str,
    target_table_path: str,
    schema: dict[str, cleaning.SchemaTypeUnion],
    ignore_fivetran_logic: bool = True,
    add_load_datetime: bool = True,
    drop_complete_duplicates: bool = False,
):
    """
    Promotes raw table to prep table.

    Parameters:
    - source_table_path: str, path to source table
    - target_table_path: str, path to target table
    - schema: dict[str, SchemaTypeUnion], schema of source table
    - ignore_fivetran_logic: bool, whether to ignore Fivetran logic
    """

    logs.log_info(f"Promoting {source_table_path} to {target_table_path}...")

    source_df = spark_utils.read_table(spark_session, source_table_path)
    target_df = spark_utils.read_table(spark_session, target_table_path, if_exists=True)

    if ignore_fivetran_logic:
        logs.log_info("Removing Fivetran logic from source...")

        source_count = source_df.count()
        source_df = fivetran_utils.remove_fivetran_logic(spark_session, source_df)

        removed_count = source_count - source_df.count()

        logs.log_success(
            f"Removing Fivetran logic from source done, removed {removed_count} records."
        )

    logs.log_info(f"Cleaning source...")

    cleaning_result = cleaning.clean_table(
        df=source_df,
        schema=schema,
        drop_complete_duplicates=drop_complete_duplicates,
    )

    if len(cleaning_result.key_cols) == 0:
        raise ValueError("No key columns found in schema")

    logs.log_success(f"Cleaning source done.")

    logs.log_info(f"Generating SCD2 table from source...")

    current_datetime = datetime.now()
    scd_df = scd2.calculate(
        spark_session=spark_session,
        source_df=cleaning_result.value,
        target_df=target_df,
        key_cols=cleaning_result.key_cols,
        current_datetime=current_datetime,
    )

    logs.log_info(f"Generating SCD2 table from source done.")

    if add_load_datetime:

        logs.log_info(f"Adding load datetime to SCD2 table...")

        scd_df = scd_df.withColumn(
            "_loaded_at", spf.lit(current_datetime).cast("timestamp")
        )

        logs.log_success(f"Adding load datetime to SCD2 table done.")

    logs.log_info(f"Writing to {target_table_path}...")

    spark_utils.write_table(spark_session, scd_df, target_table_path)

    logs.log_success(f"Writing to {target_table_path} done.")

    logs.log_success(f"Promoting {source_table_path} to {target_table_path} done.")
