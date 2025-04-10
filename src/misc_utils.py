from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame


def get_distincts(
    spark_session: SparkSession, df: DataFrame | ConnectDataFrame, limit: int = 500
) -> DataFrame | ConnectDataFrame:
    distinct_sql = f"""
        with
        { " , ".join([f"{column}_distinct_all as (select distinct {column} from {{source}} order by {column}), {column}_distinct as (select * from {column}_distinct_all limit {limit})" for column in df.columns])}

        select * from { " cross join ".join([f"{column}_distinct" for column in df.columns])}
    """

    result_df = spark_session.sql(distinct_sql, source=df)

    return result_df


def get_type_name(some_type) -> str:
    """
    Get the name of the type of a given object.

    Args:
        some_type: The object to get the type name of.

    Returns:
        str: The name of the type.
    """
    return type(some_type).__name__
