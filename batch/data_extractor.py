class DataExtractor:
    def __init__(self, spark_client, resolved_args):
        self.spark_client = spark_client
        self.resolved_args = resolved_args

    def extract(self):
        pass