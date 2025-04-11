from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame


def remove_fivetran_logic(
    spark_session: SparkSession,
    df: DataFrame | ConnectDataFrame,
) -> DataFrame | ConnectDataFrame:
    """
    Removes records stored by fivetran to store history.

    Note:
        - Currently only supports fivetran's soft delete mode.

    Example:
        In fivetran's soft delete mode, fivetran adds a new column `_fivetran_deleted` which marks if the record has been deleted from the source. This function will remove the records where the value for this column is true. This will make sure that the records are the live ones.

    Args:
        df (DataFrame | ConnectDataFrame): The DataFrame to process.

    Returns:
        DataFrame | ConnectDataFrame: A DataFrame with fivetran deleted records removed.
    """

    columns = [col for col in df.columns if not col.startswith("_fivetran")]

    return spark_session.sql(
        f"""
        select { ", ".join(columns) }
        from {{source}}
        { "where _fivetran_deleted is false" if "_fivetran_deleted" in df.columns else "" }
        """,
        source=df,
    )
