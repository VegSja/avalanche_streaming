import logging
from typing import Optional, Dict, Any
import uuid

from cassandra.cluster import Cluster, Session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import expr, from_json, col, udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)


def create_keyspace(session: Session) -> None:
    """
    Creates a Cassandra keyspace named `spark_avalanche` if it doesn't already exist.
    The keyspace uses the SimpleStrategy replication method with a replication factor of 1.

    Args:
        session: Cassandra session object used to execute the query.
    """
    logging.info(f"Create keyspace...")
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_avalanche
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )


def create_table(session: Session) -> None:
    """
    Creates the `avalanche_warning` table within the `spark_avalanche` keyspace if it doesn't exist.
    The table structure is designed to store avalanche warning data based on the VarsomAvalancheResponse data class.

    Args:
        session: Cassandra session object used to execute the query.
    """
    logging.info(f"Create table...")
    session.execute(
        """
    CREATE TABLE IF NOT EXISTS spark_avalanche.avalanche_warning (
        id UUID PRIMARY KEY,  -- Unique identifier for each record
        reg_id INT,           -- RegId
        region_id INT,        -- RegionId
        region_name TEXT,     -- RegionName (nullable)
        region_type_id INT,   -- RegionTypeId
        region_type_name TEXT, -- RegionTypeName (nullable)
        danger_level TEXT,    -- DangerLevel (nullable)
        valid_from TIMESTAMP,      -- ValidFrom (nullable)
        valid_to TIMESTAMP,        -- ValidTo (nullable)
        next_warning_time TIMESTAMP, -- NextWarningTime (nullable)
        publish_time TIMESTAMP,    -- PublishTime (nullable)
        danger_increase_time TIMESTAMP, -- DangerIncreaseTime (nullable)
        danger_decrease_time TIMESTAMP, -- DangerDecreaseTime (nullable)
        main_text TEXT,       -- MainText (nullable)
        lang_key INT          -- LangKey
    );
    """
    )


def insert_data(session: Session, **kwargs: Dict[str, Any]) -> None:
    """
    Inserts data into the `avalanche_warning` table in the Cassandra database.
    The data is passed via keyword arguments matching the table's columns.

    Args:
        session: Cassandra session object used to execute the insert query.
        kwargs: Data to be inserted, including `id`, `reg_id`, `region_id`, `region_name`, etc.

    Raises:
        Exception: If the insert query fails, an exception is logged.
    """
    logging.info("Inserting data...")

    # Extracting values from kwargs based on the `VarsomAvalancheResponse` structure
    id = kwargs.get("id")  # UUID
    reg_id = kwargs.get("reg_id")  # RegId
    region_id = kwargs.get("region_id")  # RegionId
    region_name = kwargs.get("region_name")  # RegionName
    region_type_id = kwargs.get("region_type_id")  # RegionTypeId
    region_type_name = kwargs.get("region_type_name")  # RegionTypeName
    danger_level = kwargs.get("danger_level")  # DangerLevel
    valid_from = kwargs.get("valid_from")  # ValidFrom
    valid_to = kwargs.get("valid_to")  # ValidTo
    next_warning_time = kwargs.get("next_warning_time")  # NextWarningTime
    publish_time = kwargs.get("publish_time")  # PublishTime
    danger_increase_time = kwargs.get("danger_increase_time")  # DangerIncreaseTime
    danger_decrease_time = kwargs.get("danger_decrease_time")  # DangerDecreaseTime
    main_text = kwargs.get("main_text")  # MainText
    lang_key = kwargs.get("lang_key")  # LangKey

    try:
        # SQL query to insert data into the `avalanche_warning` table
        session.execute(
            """
            INSERT INTO spark_avalanche.avalanche_warning (
                id, reg_id, region_id, region_name, region_type_id, region_type_name, danger_level,
                valid_from, valid_to, next_warning_time, publish_time, danger_increase_time, danger_decrease_time,
                main_text, lang_key
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                id,
                reg_id,
                region_id,
                region_name,
                region_type_id,
                region_type_name,
                danger_level,
                valid_from,
                valid_to,
                next_warning_time,
                publish_time,
                danger_increase_time,
                danger_decrease_time,
                main_text,
                lang_key,
            ),
        )

        logging.info(f"Data inserted for region {region_name} with ID {id}")

    except Exception as e:
        logging.error(f"Could not insert data due to {e}")


def create_spark_connection() -> Optional[SparkSession]:
    """
    Creates a Spark session with the necessary configurations for interacting with Cassandra and Kafka.

    Returns:
        SparkSession: The created Spark session object.

    Raises:
        Exception: If Spark session creation fails, an exception is logged.
    """
    s_conn: Optional[SparkSession] = None

    try:
        # Create SparkSession and load local JARs
        s_conn = (
            SparkSession.builder.appName("SparkAvalancheProcessor")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
                "org.apache.commons:commons-pool2:2.8.0,"
                "org.apache.kafka:kafka-clients:2.5.0,"
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.2",
            )
            .config("spark.cassandra.connection.host", "cassandra_db")
            .getOrCreate()
        )

        if s_conn:
            s_conn.sparkContext.setLogLevel("ERROR")
            logging.info("Spark connection created successfully!")
        else:
            raise Exception("Could not create spark session. Stopping...")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn: SparkSession) -> Optional[DataFrame]:
    """
    Connects to a Kafka stream and creates a DataFrame from the `avalanche_warning` topic.

    Args:
        spark_conn: Spark session object used to read from Kafka.

    Returns:
        DataFrame: A Spark DataFrame containing the Kafka data from the `avalanche_warning` topic.

    Raises:
        Exception: If Kafka connection fails, a warning is logged.
    """
    spark_df: Optional[DataFrame] = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "avalanche_region_warning")
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection() -> Optional[Session]:
    """
    Creates a connection to the Cassandra cluster.

    Returns:
        cassandra.cluster.Session: A session object for interacting with Cassandra.

    Raises:
        Exception: If Cassandra connection fails, an exception is logged.
    """
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(["cassandra_db"])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df: DataFrame) -> DataFrame:
    """
    Processes the Kafka DataFrame and extracts the relevant fields from the JSON payload.
    It applies the schema corresponding to the VarsomAvalancheResponse data class.

    Args:
        spark_df: Spark DataFrame containing Kafka data.

    Returns:
        DataFrame: A processed DataFrame with the selected fields from the Kafka JSON payload.
    """
    # Define the schema based on the VarsomAvalancheResponse class (avalanche_warning table)
    schema = StructType(
        [
            StructField("RegId", IntegerType(), False),
            StructField("RegionId", IntegerType(), False),
            StructField("RegionName", StringType(), True),
            StructField("RegionTypeId", IntegerType(), False),
            StructField("RegionTypeName", StringType(), True),
            StructField("DangerLevel", StringType(), True),
            StructField("ValidFrom", TimestampType(), True),
            StructField("ValidTo", TimestampType(), True),
            StructField("NextWarningTime", TimestampType(), True),
            StructField("PublishTime", TimestampType(), True),
            StructField("DangerIncreaseTime", TimestampType(), True),
            StructField("DangerDecreaseTime", TimestampType(), True),
            StructField("MainText", StringType(), True),
            StructField("LangKey", IntegerType(), False),
        ]
    )

    # Process Kafka data and apply the schema
    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )  # Flatten the nested struct

    return sel


uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Renames columns from Kafka's camelCase format to Cassandra's snake_case format.

    Args:
        df (DataFrame): The Spark DataFrame with original Kafka column names.

    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """
    return (
        df.withColumnRenamed("RegId", "reg_id")
        .withColumnRenamed("RegionId", "region_id")
        .withColumnRenamed("RegionName", "region_name")
        .withColumnRenamed("RegionTypeId", "region_type_id")
        .withColumnRenamed("RegionTypeName", "region_type_name")
        .withColumnRenamed("DangerLevel", "danger_level")
        .withColumnRenamed("ValidFrom", "valid_from")
        .withColumnRenamed("ValidTo", "valid_to")
        .withColumnRenamed("NextWarningTime", "next_warning_time")
        .withColumnRenamed("PublishTime", "publish_time")
        .withColumnRenamed("DangerIncreaseTime", "danger_increase_time")
        .withColumnRenamed("DangerDecreaseTime", "danger_decrease_time")
        .withColumnRenamed("MainText", "main_text")
        .withColumnRenamed("LangKey", "lang_key")
        .withColumn("id", uuid_udf())
    )  # ⬅ Generate UUID properly


if __name__ == "__main__":
    try:
        spark_conn = create_spark_connection()
        if spark_conn is None:
            raise Exception("Spark connection failed!")

        spark_df = connect_to_kafka(spark_conn)
        if spark_df is None:
            raise Exception("Kafka connection failed!")

        selection_df = create_selection_df_from_kafka(spark_df)
        selection_df = rename_columns(selection_df)

        session = create_cassandra_connection()
        if session is None:
            raise Exception("Cassandra connection failed!")

        create_keyspace(session)
        create_table(session)

        logging.info("Streaming is being started...")

        streaming_query = (
            selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("keyspace", "spark_avalanche")
            .option("table", "avalanche_warning")
            .start()
        )

        streaming_query.awaitTermination()

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}", exc_info=True)
