import sys
from spark_client import SparkClient
from analysis_config import get_analysis_config
import common_batch_job

resolved_args = {}


class DriverJob:
    def __init__(self, resolved_args:dict):
        self.analysis_number = resolved_args.get("analysis_number", None)
        self.resolved_args = resolved_args

    def start_job(self):
        try:
            self.execute_job()
        except Exception as ex:
            raise ex

    def execute_job(self):
        spark_client = None
        try:
            print(f"Job started for {self.analysis_number}")
            spark_client = SparkClient()
            analysis_config = get_analysis_config(self.analysis_number)
            self.resolved_args['analysis_config'] = analysis_config
            print(analysis_config)
            batch_job = common_batch_job.CommonBatchJob(spark_client, resolved_args)
            batch_job.execute_job()
        except Exception as ex:
            raise ex
        finally:
            if spark_client:
                spark_client.close_spark_session()

# it obtains provided arguments through spark-submit
def get_args():
    for param in sys.argv:
         if '=' in param:
             key_value = param.split("=")
             key = param.split("=")[0]
             value = param.split("=")[1]
             resolved_args[key] = value

    return resolved_args


if __name__ == '__main__':
    print("hello World")
    DriverJob(get_args()).start_job()
