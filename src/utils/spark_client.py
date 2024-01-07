from pyspark.sql.session import SparkSession

class SparkClient:
    def __init__(self):
        print("Initializing Spark Session.")
        try:
            self.spark = SparkSession.builder \
                .appName("BCG Case Study") \
                .master('local[*]') \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel('ERROR')
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

    def read_csv_file(self, path: str):
        print(path)
        try:
            df = self.spark.read.format('csv').option("header", "true") \
            .option("inferSchema", "true").load(path)

            return df
        except Exception as ex:
            print(f"Unable to load from {path}, due to {ex}")
            raise ex

    def read_spark_table(self, table_name: str):
        df = self.spark.read.table(table_name)
        return df