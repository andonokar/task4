import pyspark.sql
from sparkInit import spark
from pyspark.sql import functions as f


def clean_transform(sdf: pyspark.sql.DataFrame) -> tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame]:
    df_silver = sdf.withColumn("campaign", f.split(f.col("campaign"), ' ').getItem(0)) \
        .withColumn("cleaning_time", f.current_timestamp()) \
        .withColumn("location", f.concat(f.col("region"), f.lit(" - "), f.col("country"))) \
        .select("advertiser", "campaign", "event_name", "gender", "income", "cleaning_time",
                "page_url", "location", "user_id", "region")

    df_silver_ok = df_silver.where(
        (f.col("gender") != "unknown") & (f.col("income") != "unknown") & (f.col("region") != "undefined")
    ).drop("region")

    df_silver_nok = df_silver.where(
        (f.col("gender") == "unknown") | (f.col("income") == "unknown") | (f.col("region") == "undefined")
    ).drop("region")
    return df_silver_ok, df_silver_nok


def main():
    df = spark.read.format("mongodb") \
        .option("spark.mongodb.collection", "bronze") \
        .load()

    df_silver_ok, df_silver_nok = clean_transform(df)

    df_silver_ok.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.collection", "silver") \
        .save()

    df_silver_nok.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.collection", "silverNOk") \
        .save()


if __name__ == "__main__":
    main()
