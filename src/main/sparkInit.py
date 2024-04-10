from pyspark.sql import SparkSession


def startspark():
    builder = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,"
                               "org.apache.kafka:kafka-clients:3.2.1,"
                               "org.mongodb.spark:mongo-spark-connector_2.12:10.2.2"
    ) \
        .config("spark.mongodb.connection.uri", "mongodb://root:example@mongodb:27017/") \
        .config("spark.mongodb.database", "task4") \
        .master("local[*]")

    sparksession = builder.getOrCreate()
    return sparksession


spark = startspark()
