from base_batch_job import BaseBatchJob
from project_utils import Analysis
from source_config import get_source_config
from transform_methods import *


class CommonBatchJob(BaseBatchJob):
    def __init__(self, spark_client, resolved_args: dict):
        super().__init__(spark_client, resolved_args)
        self.analysis_number = self.resolved_args.get("analysis_number", None).lower()
        self.analysis_config = self.resolved_args.get("analysis_config", None)
        self.input_path = self.resolved_args.get("input_path", "input_data")
        self.output_path = self.resolved_args.get("output_path", "output")

    def execute_job(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        print("Extraction of required files [STARTED]")
        source_data = self.analysis_config.source_data
        if source_data:
            for file in source_data:
                print(f"Extraction process started for {file}")
                df = self.spark_client.read_csv_file(self.input_path + '/' + f"{file}.csv")

                # I have seen that few tables have some duplicate records, to remove that it is added
                if file in [InputData.Units.value]:
                    df = df.dropDuplicates(["CRASH_ID", "UNIT_NBR"])

                df.createOrReplaceTempView(file)
        else:
            raise Exception(f"Source tables are not provider for: {self.resolved_args.get('analysis_number')}")

    def transform(self):
        result = None
        print("Transformation [STARTED]")
        if self.analysis_number == Analysis.Analysis_1.value:
            result = transformation_for_analysis_1(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_2.value:
            result = transformation_for_analysis_2(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_3.value:
            result = transformation_for_analysis_3(self.spark_client, self.analysis_config)
        else:
            print("Invalid Analysis Number!!!")

        if type(result) == int:
            self.result_str = f"Required final count for this analysis is: {result}"
        else:
            self.df_result = result

    def load(self):
        print("Loading [STARTED]")

        if self.result_str:
            print(f"Loading the result as text file into the given output path: {self.output_path}")
            text_file = open(f"{self.output_path}/{self.analysis_number}.txt", "w")
            n = text_file.write(self.result_str)
            text_file.close()
            print(f"Loading the result as text file into the given output path: {self.output_path} is [COMPLETED]")

        elif self.df_result:
            print(f"Saving the final dataframe as csv file into the given output path: {self.output_path}")

            self.df_result.coalesce(1).write.format('csv').mode('overwrite') \
            .option("header", "true").save(f"{self.output_path}/{self.analysis_number}/")

            print(f"Saving the final dataframe as csv file into the given output path: {self.output_path} is [COMPLETED]")