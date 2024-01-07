from abc import ABC
from data_extractor import DataExtractor
from data_transformer import DataTransformer
from data_loader import DataLoader

class BaseBatchJob(ABC):
    def __init__(self, spark_client, resolved_args):
        self.resolved_args = resolved_args
        self.spark_client = spark_client
        self.data_extractor = DataExtractor(spark_client, resolved_args)
        self.data_transformer = DataTransformer(spark_client, resolved_args)
        self.data_loader = DataLoader(spark_client, resolved_args)