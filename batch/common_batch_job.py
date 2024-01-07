from base_batch_job import BaseBatchJob

class CommonBatchJob(BaseBatchJob):
    def __init__(self, spark_client, resolved_args: dict):
        super().__init__(spark_client, resolved_args)

    def execute_job(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        print("Extraction of required files [STARTED]")
        self.data_extractor.extract()

    def transform(self):
        print("Transformation [STARTED]")
        self.data_transformer.transform()

    def load(self):
        print("Loading [STARTED]")
        self.data_loader.load()