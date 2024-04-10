import unittest
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from src.main import silver


class SilverTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_clean_transform(self):
        schemasilver = t.StructType([
            t.StructField("user_id", t.StringType(), True),
            t.StructField("event_name", t.StringType(), True),
            t.StructField("advertiser", t.StringType(), True),
            t.StructField("campaign", t.StringType(), True),
            t.StructField("gender", t.StringType(), True),
            t.StructField("income", t.StringType(), True),
            t.StructField("page_url", t.StringType(), True),
            t.StructField("region", t.StringType(), True),
            t.StructField("country", t.StringType(), True)
        ])
        data = [("9668656013", "Impression", "Dollar Tree", "3 9", "Female", "100k+", "/", "Florida", "US"),
                ("9668656013", "Impression", "Dollar Tree", "3 9", "unknown", "100k+", "/", "Florida", "US"),
                ("9668656013", "Impression", "Dollar Tree", "3 9", "Female", "unknown", "/", "Florida", "US"),
                ("9668656013", "Impression", "Dollar Tree", "3 9", "Female", "100k+", "/", "undefined", "US")]
        df = self.spark.createDataFrame(data, schemasilver)
        silver_ok, silver_not_ok = silver.clean_transform(df)
        silver_row = silver_ok.collect()[0]
        self.assertEqual("3", silver_row.campaign)
        self.assertEqual(3, silver_not_ok.count())


if __name__ == '__main__':
    unittest.main()
