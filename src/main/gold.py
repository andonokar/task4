import pyspark.sql
from sparkInit import spark
from pyspark.sql import functions as f
from itertools import chain


def gold_processing(gdf: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    dfs_to_merge = []
    gdf.cache()
    df_cache = gdf.groupby("advertiser")

    df_event_name = df_cache.pivot("event_name").count()
    dfs_to_merge.append(
        df_event_name
        .withColumn("CTR", f.col("Click") / f.col("Impression"))
        .withColumn("ConversionRate", f.col("`Downstream Conversion`") / f.col("Impression"))
        .withColumn("ConversionClickRate", f.col("`Downstream Conversion`") / f.col("Click")))

    df_gender = df_cache.pivot("gender").count()
    dfs_to_merge.append(
        df_gender
        .withColumn("maleFemaleRate", f.col("Male") / f.col("Female"))
        .select("advertiser", "maleFemaleRate"))

    df_rate = df_cache.pivot("income").count()
    c_rate = [c for c in df_rate.columns if c != "advertiser"]
    for column in c_rate:
        df_rate = df_rate.withColumn(
            f"{column}Rate",
            f.col(f"`{column}`") / (f.col(f"`{c_rate[0]}`") + f.col(f"`{c_rate[1]}`") + f.col(f"`{c_rate[2]}`") +
                                    f.col(f"`{c_rate[3]}`") + f.col(f"`{c_rate[4]}`")))
    dfs_to_merge.append(df_rate.select(["advertiser"] + [f"`{c}Rate`" for c in c_rate]))

    columns_max = {
        "location": ("cityEvents", "cityMostEngaged"),
        "campaign": ("campaignEvents", "campaignMostEngaged")
    }
    for column, setup in columns_max.items():
        df_column = df_cache.pivot(column).count()
        df_column_max = df_column.withColumn(
            setup[0], f.greatest(*[f.col(f"`{c}`") for c in df_column.columns if c != "advertiser"])
        )
        df_set = df_column_max.select(
            "*", f.posexplode(f.create_map(list(chain(*[
                (f.lit(c), f.col(f"`{c}`")) for c in df_column_max.columns if c not in ["advertiser", setup[0]]
            ])))))
        df_set.cache()
        dfs_to_merge.append(df_set
                            .filter(f.col(setup[0]) == f.col("value"))
                            .select(f.col("advertiser"), f.col("key").alias(setup[1]), setup[0]))

    final_df = dfs_to_merge[0]
    for dfs in dfs_to_merge[1:]:
        final_df = final_df.join(dfs, on="advertiser", how="left")
    final_df = final_df.withColumnRenamed("Downstream Conversion", "DownstreamConversion") \
        .withColumnRenamed("25k - 50kRate", "25k-50kRate") \
        .withColumnRenamed("25k and belowRate", "25kAndBelowRate") \
        .withColumnRenamed("50k - 75kRate", "50k-75kRate") \
        .withColumnRenamed("75k - 99kRate", "75k-99kRate") \
        .withColumn("agg_time", f.current_timestamp()) \
        .dropDuplicates(["advertiser"])
    return final_df


def main():
    df = spark.read.format("mongodb") \
        .option("spark.mongodb.collection", "silver") \
        .load()

    gold_df = gold_processing(df)

    gold_df.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("spark.mongodb.collection", "gold") \
        .save()


if __name__ == "__main__":
    main()
