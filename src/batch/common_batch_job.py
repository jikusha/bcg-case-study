from project_utils import Analysis, cache_required_list
from transform_methods import *
from spark_client import load_data_into_output


class CommonBatchJob:
    def __init__(self, spark_client, resolved_args: dict):
        self.resolved_args = resolved_args
        self.spark_client = spark_client
        self.result_dict = None
        self.analysis_number = self.resolved_args.get("analysis_number", None).lower().strip()
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

                # when executing all analysis at once
                # few df s are cached, as they are used multiple times later
                if self.analysis_number == 'all' and file in cache_required_list:
                    print("Caching started ==>")
                    df.cache()

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
        elif self.analysis_number == Analysis.Analysis_4.value:
            result = transformation_for_analysis_4(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_5.value:
            result = transformation_for_analysis_5(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_6.value:
            result = transformation_for_analysis_6(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_7.value:
            result = transformation_for_analysis_7(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_8.value:
            result = transformation_for_analysis_8(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_9.value:
            result = transformation_for_analysis_9(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.Analysis_10.value:
            result = transformation_for_analysis_10(self.spark_client, self.analysis_config)
        elif self.analysis_number == Analysis.ALL.value:
            result = transformation_for_all_analysis(self.spark_client, self.analysis_config)
        else:
            print("Invalid Analysis Number!!!")

        self.result_dict = result

    def load(self):
        print("Loading [STARTED]")

        if self.result_dict:
            load_data_into_output(self.result_dict, self.output_path)
        else:
            print("No output generated to load !!!!!!")