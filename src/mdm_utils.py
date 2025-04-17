from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame


def select_masters_by_power(
    spark_session: SparkSession,
    df: DataFrame | ConnectDataFrame,
    cluster_col: str,
    key_cols: list[str],
    powers: dict[str, int],
    master_col_prefix: str = "master_",
) -> DataFrame | ConnectDataFrame:

    default_power = powers.get("*", 1)
    non_key_cols = [col for col in df.columns if col not in key_cols]

    # Sql query to pick a master record
    master_sql = f"""
        with power as (
            select
                { ", ".join(key_cols) }
                , {cluster_col}
                , { " + ".join([
                    f"(case when {column} is not null then {powers.get(column, default_power)} else 0 end)"
                    for column in df.columns
                ]) } as power
            from {{cluster}}
        ),
        ranks as (
            select
                { ", ".join(key_cols) }
                , {cluster_col}
                , row_number() over (partition by {cluster_col} order by power desc) as rank
            from power
        )

        select
            { ", ".join([f"{{cluster}}.{column}" for column in key_cols]) }
            , { ", ".join([f"ranks.{column} as {master_col_prefix}{column}" for column in key_cols]) }
            , { ", ".join([f"{{cluster}}.{column}" for column in non_key_cols]) }
        from {{cluster}}
        left join ranks on {{cluster}}.{cluster_col} = ranks.{cluster_col} and ranks.rank = 1
    """

    master_df = spark_session.sql(
        master_sql,
        cluster=df,
    )

    return master_df
