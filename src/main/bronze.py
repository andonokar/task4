import pyspark.sql

from sparkInit import spark
from pyspark.sql import functions as f
from pyspark.sql import types as t


def ingest_data(dfs: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    # df_data = df.selectExpr("CAST(value AS STRING) AS ascii_data")
    # final_df = df_data.selectExpr(
    #     "split(ascii_data, '\t')[0] AS user_id",
    #     "split(ascii_data, '\t')[1] AS event_name",
    #     "split(ascii_data, '\t')[2] AS advertiser",
    #     "split(ascii_data, '\t')[3] AS campaign",
    #     "split(ascii_data, '\t')[4] AS gender",
    #     "split(ascii_data, '\t')[5] AS income",
    #     "split(ascii_data, '\t')[6] AS page_url",
    #     "split(ascii_data, '\t')[7] AS region",
    #     "split(ascii_data, '\t')[8] AS country",
    #     "CURRENT_TIMESTAMP() AS ingestion_time"
    # )
    df_data = dfs.withColumn("ascii_data", f.col("value").cast(t.StringType()))

    final_df = df_data \
        .withColumn("user_id", f.split(f.col("ascii_data"), '\t').getItem(0)) \
        .withColumn("event_name", f.split(f.col("ascii_data"), '\t').getItem(1)) \
        .withColumn("advertiser", f.split(f.col("ascii_data"), '\t').getItem(2)) \
        .withColumn("campaign", f.split(f.col("ascii_data"), '\t').getItem(3)) \
        .withColumn("gender", f.split(f.col("ascii_data"), '\t').getItem(4)) \
        .withColumn("income", f.split(f.col("ascii_data"), '\t').getItem(5)) \
        .withColumn("page_url", f.split(f.col("ascii_data"), '\t').getItem(6)) \
        .withColumn("region", f.split(f.col("ascii_data"), '\t').getItem(7)) \
        .withColumn("country", f.split(f.col("ascii_data"), '\t').getItem(8)) \
        .withColumn("ingestion_time", f.current_timestamp())

    return final_df.select("user_id", "event_name", "advertiser", "campaign", "gender", "income",
                           "page_url", "region", "country", "ingestion_time")


def main():
    topic = "ad_events"

    kafka_params = {
        "bootstrap.servers": "public-kafka.memcompute.com:9092",
        "group.id": "liander",
        "auto.offset.reset": "earliest"
    }

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_params["bootstrap.servers"]) \
        .option("subscribe", topic) \
        .option("group.id", kafka_params["group.id"]) \
        .option("auto.offset.reset", kafka_params["auto.offset.reset"]) \
        .load()

    final_df = ingest_data(df)

    final_df \
        .writeStream \
        .outputMode("append") \
        .format("mongodb") \
        .option("checkpointLocation", "/tmp/pyspark/bronze/") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .option("spark.mongodb.collection", "bronze") \
        .start()


if __name__ == "__main__":
    main()
