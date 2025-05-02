from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
from splink import DuckDBAPI, Linker, SettingsCreator
from . import spark_utils


class MasterRecordSelector(ABC):

    @abstractmethod
    def select_masters(
        self,
        spark_session: SparkSession | ConnectSparkSession,
        df: DataFrame | ConnectDataFrame,
        cluster_col: str,
        key_cols: list[str],
        master_col_prefix: str = "master_",
    ) -> DataFrame | ConnectDataFrame:
        pass


@dataclass
class PowerMasterRecordSelector(MasterRecordSelector):

    DEFAULT_POWER = 1

    powers: dict[str, int] = field(default_factory=dict)


    def select_masters(
        self,
        spark_session: SparkSession | ConnectSparkSession,
        df: DataFrame | ConnectDataFrame,
        cluster_col: str,
        key_cols: list[str],
        master_col_prefix: str = "master_",
    ) -> DataFrame | ConnectDataFrame:

        """
        Abstract method to select master records from a DataFrame or ConnectDataFrame.

        :param df: The input DataFrame or ConnectDataFrame.
        :param cluster_col: The col name representing the cluster.
        :param key_cols: List of key col names.
        :return: A DataFrame or ConnectDataFrame with master records selected.
        """

        default_power = self.powers.get("*", self.DEFAULT_POWER)
        non_key_cols = [col for col in df.columns if col not in key_cols]

        # Sql query to pick a master record
        master_sql = f"""
            with power as (
                select
                    { ", ".join(key_cols) }
                    , {cluster_col}
                    , { " + ".join([
                        f"(case when {col} is not null then {self.powers.get(col, default_power)} else 0 end)"
                        for col in df.columns if col != cluster_col
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
                { ", ".join([f"{{cluster}}.{col}" for col in key_cols]) }
                , { ", ".join([f"ranks.{col} as {master_col_prefix}{col}" for col in key_cols]) }
                , { ", ".join([f"{{cluster}}.{col}" for col in non_key_cols]) }
            from {{cluster}}
            left join ranks on {{cluster}}.{cluster_col} = ranks.{cluster_col} and ranks.rank = 1
        """

        master_df = spark_session.sql(
            master_sql,
            cluster=df,
        )

        return master_df

def _validate_key_cols(
    spark_session: SparkSession | ConnectSparkSession,
    df: DataFrame | ConnectDataFrame,
    key_cols: list[str],
) -> bool:
    """
    Validate key columns in the DataFrame or ConnectDataFrame.

    :param df: The input DataFrame or ConnectDataFrame.
    :param key_cols: List of key col names.
    :return: True if all key columns are present in the DataFrame or ConnectDataFrame, False otherwise.
    """

    # Check if key columns as composite key represent unique records
    composite_key_check_df = df.groupBy(key_cols).count().filter("count > 1")

    # If there are duplicate records, the count will not be 0
    return composite_key_check_df.count() == 0


def generate_mdm(
    spark_session: SparkSession | ConnectSparkSession,
    df: DataFrame | ConnectDataFrame,
    key_cols: list[str],
    blocking_rules: list,
    comparisions: list,
    master_record_selector: MasterRecordSelector,
    db_api = DuckDBAPI(),
    threshold_match_probability = .9,
    master_col_prefix: str = "master_",
) -> DataFrame | ConnectDataFrame:
    """
    Generate master records using the PowerMasterRecordSelector.

    :param spark: Spark session.
    :param df: Source DataFrame or ConnectDataFrame.
    :param cluster_col: Column name representing the cluster.
    :param key_cols: List of key col names.
    :param powers: Dictionary of col powers.
    :param master_col_prefix: Prefix for master col names.
    :return: DataFrame or ConnectDataFrame with master records selected.
    """

    assert isinstance(spark_session, (SparkSession, ConnectSparkSession)), "spark_session must be a SparkSession or ConnectSparkSession instance"
    assert isinstance(df, (DataFrame, ConnectDataFrame)), "df must be a DataFrame or ConnectDataFrame instance"
    assert isinstance(key_cols, list) and all(isinstance(col, str) for col in key_cols), "key_cols must be a list of strings"
    assert len(key_cols) > 0, "key_cols must contain at least one column"
    assert isinstance(blocking_rules, list), "blocking_rules must be a list"
    assert isinstance(comparisions, list), "comparisions must be a list"
    assert isinstance(master_record_selector, MasterRecordSelector), "master_record_selector must be an instance of MasterRecordSelector"
    assert isinstance(threshold_match_probability, (float, int)), "threshold_match_probability must be a float or int"
    assert isinstance(master_col_prefix, str), "master_col_prefix must be a string"
    assert _validate_key_cols(spark_session, df, key_cols), "key_cols must represent unique records"

    # --------------------------------------------------------------------------
    # Generate a unique id col for splink, splink does not support
    # composite keys
    # --------------------------------------------------------------------------

    if len(key_cols) == 1:
        unique_id_col_name = key_cols[0]
    else:
        unique_id_col_name = spark_utils.get_unique_col_name(
            cols=df.columns,
            name_hint="composite_key",
        )

        df = spark_utils.combine_composite_key(
            spark_session=spark_session,
            df=df,
            key_cols=key_cols,
            composite_key_col=unique_id_col_name
        )

    # --------------------------------------------------------------------------
    # Prepare inputs for the model
    # --------------------------------------------------------------------------

    source_pandas_df = df.toPandas()
    other_cols = [col for col in df.columns if col not in key_cols]

    # Defining the match configuration
    settings = SettingsCreator(
        unique_id_column_name=unique_id_col_name,
        link_type="dedupe_only",
        comparisons=comparisions,
        blocking_rules_to_generate_predictions=blocking_rules,
    )

    if db_api is None:
        db_api = DuckDBAPI()

    # Create the linker to perform training and prediction
    linker = Linker(source_pandas_df, settings, db_api)

    # --------------------------------------------------------------------------
    # Perform matches
    # --------------------------------------------------------------------------

    # Make predictions
    prediction_splink_df = linker.inference.predict()

    # Create clusters based on the predictions
    cluster_splink_df = linker.clustering.cluster_pairwise_predictions_at_threshold(
        prediction_splink_df,
        threshold_match_probability=threshold_match_probability,
    )

    # Saving the result as spark dataframe
    cluster_pandas_df = cluster_splink_df.as_pandas_dataframe()
    cluster_df = spark_session.createDataFrame(cluster_pandas_df)

    master_df = cluster_df

    # We no longer need the composite key, so drop it if created before
    # for splink
    if len(key_cols) > 1:
        cluster_df = cluster_df.drop(unique_id_col_name)

    # --------------------------------------------------------------------------
    # Select master records
    # --------------------------------------------------------------------------

    # Select the master records
    master_df = master_record_selector.select_masters(
        spark_session=spark_session,
        df=cluster_df,
        key_cols=key_cols,
        cluster_col="cluster_id",
        master_col_prefix=master_col_prefix,
    )

    # We no longer need the cluster_id col, as the master records have
    # been selected.
    master_df = master_df.drop("cluster_id")

    return master_df
