from pyspark.sql.session import SparkSession

class SparkClient:
    def __init__(self):
        print("Initializing Spark Session.")
        try:
            self.spark = SparkSession.builder \
                .appName("BCG Case Study") \
                .master('local[*]') \
                .getOrCreate()
        except Exception as ex:
            print(f"Failed to initialize Spark Session due to {ex}")
            raise ex

        print("Spark Session is initialized and available for use")

    def get_spark(self):
        return self.spark

    def close_spark_session(self):
        try:
            self.spark.stop()
            print("Spark session is closed.")
        except Exception as ex:
            print("Failed to close Spark session.")
            raise ex