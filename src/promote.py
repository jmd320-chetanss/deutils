# --------------------------------------------------------------------------------------------------
# This file contains the logic to promote a table from `raw` layer to `prep` layer.
# --------------------------------------------------------------------------------------------------

from pyspark.sql import SparkSession
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
):
    """
    Promotes raw table to prep table.

    Parameters:
    - source_table_path: str, path to source table
    - target_table_path: str, path to target table
    - schema: dict[str, SchemaTypeUnion], schema of source table
    - ignore_fivetran_logic: bool, whether to ignore Fivetran logic
    """

    key_cols = cleaning.get_renamed_key_columns(schema)
    assert len(key_cols) > 0, "No key columns found in schema"

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

    cleaned_df = cleaning.clean_table(
        df=source_df,
        schema=schema,
    )

    scd_df = scd2.calculate(
        spark_session=spark_session,
        source_df=cleaned_df,
        target_df=target_df,
        key_cols=key_cols,
    )

    spark_utils.write_table(spark_session, scd_df, target_table_path)
