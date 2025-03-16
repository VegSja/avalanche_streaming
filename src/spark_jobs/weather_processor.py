import logging
import uuid
from typing import Optional

from cassandra.cluster import Cluster, Session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, element_at, from_json, udf
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

logging.basicConfig(level=logging.INFO)


def create_keyspace(session: Session) -> None:
    logging.info("Create keyspace...")
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_weather
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )


def create_table(session: Session) -> None:
    logging.info("Create table...")
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_weather.weather_forecast (
            id UUID PRIMARY KEY,
            region_id TEXT,
            region_name TEXT,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            latitude DOUBLE,
            longitude DOUBLE,
            elevation INT,
            time TIMESTAMP,
            weathercode INT,
            temperature_2m_max DOUBLE,
            temperature_2m_min DOUBLE,
            temperature_2m_mean DOUBLE,
            rain_sum DOUBLE,
            snowfall_sum DOUBLE,
            precipitation_hours DOUBLE,
            windspeed_10m_max DOUBLE,
            windgusts_10m_max DOUBLE,
            winddirection_10m_dominant DOUBLE
        );
        """
    )


def create_spark_connection() -> Optional[SparkSession]:
    try:
        s_conn = (
            SparkSession.builder.appName("SparkWeatherProcessor")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
            )
            .config("spark.cassandra.connection.host", "cassandra_db")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
        return None


def connect_to_kafka(spark_conn: SparkSession) -> Optional[DataFrame]:
    try:
        return (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", "weather_forecast")
            .option("startingOffsets", "earliest")
            .load()
        )
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
        return None


def create_cassandra_connection() -> Optional[Session]:
    try:
        cluster = Cluster(["cassandra_db"])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df: DataFrame) -> DataFrame:
    schema = StructType(
        [
            StructField("region_id", StringType(), False),
            StructField("region_name", StringType(), True),
            StructField("start_date", TimestampType(), True),
            StructField("end_date", TimestampType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("elevation", IntegerType(), True),
            StructField(
                "daily",
                StructType(
                    [
                        StructField("time", ArrayType(StringType()), True),
                        StructField("weathercode", ArrayType(IntegerType()), True),
                        StructField(
                            "temperature_2m_max", ArrayType(DoubleType()), True
                        ),
                        StructField(
                            "temperature_2m_min", ArrayType(DoubleType()), True
                        ),
                        StructField(
                            "temperature_2m_mean", ArrayType(DoubleType()), True
                        ),
                        StructField("rain_sum", ArrayType(DoubleType()), True),
                        StructField("snowfall_sum", ArrayType(DoubleType()), True),
                        StructField(
                            "precipitation_hours", ArrayType(DoubleType()), True
                        ),
                        StructField("windspeed_10m_max", ArrayType(DoubleType()), True),
                        StructField("windgusts_10m_max", ArrayType(DoubleType()), True),
                        StructField(
                            "winddirection_10m_dominant", ArrayType(DoubleType()), True
                        ),
                    ]
                ),
                True,
            ),
        ]
    )

    sel = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select(
            col("data.region_id"),
            col("data.region_name"),
            col("data.start_date"),
            col("data.end_date"),
            col("data.latitude"),
            col("data.longitude"),
            col("data.elevation"),
            element_at(col("data.daily.time"), 1).alias("time"),
            element_at(col("data.daily.weathercode"), 1)
            .cast(IntegerType())
            .alias("weathercode"),
            element_at(col("data.daily.temperature_2m_max"), 1)
            .cast(DoubleType())
            .alias("temperature_2m_max"),
            element_at(col("data.daily.temperature_2m_min"), 1)
            .cast(DoubleType())
            .alias("temperature_2m_min"),
            element_at(col("data.daily.temperature_2m_mean"), 1)
            .cast(DoubleType())
            .alias("temperature_2m_mean"),
            element_at(col("data.daily.rain_sum"), 1)
            .cast(DoubleType())
            .alias("rain_sum"),
            element_at(col("data.daily.snowfall_sum"), 1)
            .cast(DoubleType())
            .alias("snowfall_sum"),
            element_at(col("data.daily.precipitation_hours"), 1)
            .cast(DoubleType())
            .alias("precipitation_hours"),
            element_at(col("data.daily.windspeed_10m_max"), 1)
            .cast(DoubleType())
            .alias("windspeed_10m_max"),
            element_at(col("data.daily.windgusts_10m_max"), 1)
            .cast(DoubleType())
            .alias("windgusts_10m_max"),
            element_at(col("data.daily.winddirection_10m_dominant"), 1)
            .cast(DoubleType())
            .alias("winddirection_10m_dominant"),
        )
    )
    return sel


uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())


def add_id_column(df: DataFrame) -> DataFrame:
    return df.withColumn("id", uuid_udf())


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    spark_df = connect_to_kafka(spark_conn)
    selection_df = create_selection_df_from_kafka(spark_df)
    selection_df = add_id_column(selection_df)

    session = create_cassandra_connection()
    create_keyspace(session)
    create_table(session)

    logging.info("Streaming is being started...")
    streaming_query = (
        selection_df.writeStream.format("org.apache.spark.sql.cassandra")
        .option("checkpointLocation", "/tmp/checkpoint")
        .option("keyspace", "spark_weather")
        .option("table", "weather_forecast")
        .start()
    )
    streaming_query.awaitTermination()
