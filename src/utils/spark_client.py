from pyspark.sql import DataFrame
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

def load_data_into_output(result_dict: dict, output_path: str):
    for k,v in result_dict.items():
        output_path_final = f"{output_path}/{k}"
        if type(v) == int:
            result_str = f"Required final count for this analysis is: {v}"
            write_text_file(result_str, f"{output_path_final}.txt")
        else:
            load_df_as_csv(v, f"{output_path_final}/")

def write_text_file(result_str: str, output_path: str):
    print(f"Loading the result as text file into the given output path: {output_path}")
    text_file = open(output_path, "w")
    n = text_file.write(result_str)
    text_file.close()
    print(f"Loading the result as text file into the given output path: {output_path} is [COMPLETED]")

def load_df_as_csv(df: DataFrame, output_path: str):
    print(f"Saving the final dataframe as csv file into the given output path: {output_path}")

    df.coalesce(1).write.format('csv').mode('overwrite') \
    .option("header", "true").save(output_path)

    print(f"Saving the final dataframe as csv file into the given output path: {output_path} is [COMPLETED]")

