import unittest
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from src.main import gold


class GoldTest(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_clean_transform(self):
        gold_schema = t.StructType([
            t.StructField("user_id", t.StringType(), True),
            t.StructField("event_name", t.StringType(), True),
            t.StructField("advertiser", t.StringType(), True),
            t.StructField("campaign", t.StringType(), True),
            t.StructField("gender", t.StringType(), True),
            t.StructField("income", t.StringType(), True),
            t.StructField("page_url", t.StringType(), True),
            t.StructField("location", t.StringType(), True)
        ])
        data = [("9668656013", "Impression", "Dollar Tree", "3", "Female", "3k", "/", "Florida - US"),
                ("9668656013", "Click", "Dollar Tree", "3", "Male", "7k", "/", "Florida - US"),
                ("9668656013", "Downstream Conversion", "Dollar Tree", "5", "Male", "25-50k", "/",
                 "Florida - US"),
                ("9668656013", "Click", "Dollar Tree", "4", "Male", "100k+", "/", "California - US"),
                ("9668656013", "Click", "Dollar Tree", "4", "Female", "3223k+", "/", "California - US")]
        df = self.spark.createDataFrame(data, gold_schema)
        gold_df = gold.gold_processing(df)
        gold_row = gold_df.collect()[0]
        self.assertEqual(1, gold_df.count())
        self.assertEqual(1.5, gold_row.maleFemaleRate)
        self.assertEqual("Dollar Tree", gold_row.advertiser)
        self.assertEqual("Florida - US", gold_row.cityMostEngaged)


if __name__ == '__main__':
    unittest.main()
