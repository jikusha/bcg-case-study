import sys
from spark_client import SparkClient
from analysis_config import get_analysis_config
import common_batch_job

resolved_args = {}


class DriverJob:
    def __init__(self, resolved_args:dict):
        self.analysis_number = resolved_args.get("analysis_number", None).lower().strip()
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
            batch_job = common_batch_job.CommonBatchJob(spark_client, self.resolved_args)
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
             key = key_value[0]
             value = key_value[1]
             resolved_args[key] = value

    return resolved_args


if __name__ == '__main__':
    resolved_args = get_args()
    analysis_number = resolved_args.get("analysis_number", None)
    if analysis_number and (analysis_number.lower().strip().startswith('analysis') or analysis_number.lower().strip() == 'all'):
        DriverJob(resolved_args).start_job()
    else:
        print("Invalid Analysis Number!!")
        raise Exception("Invalid Analysis Number!!")
