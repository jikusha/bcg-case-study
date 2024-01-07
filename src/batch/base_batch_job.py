from abc import ABC


class BaseBatchJob(ABC):
    def __init__(self, spark_client, resolved_args):
        self.resolved_args = resolved_args
        self.spark_client = spark_client
        self.result_str = None
        self.df_result = None