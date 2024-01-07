from analysis_config import ConfigEncoder
from project_utils import *
import json

class SourceConfig:
    def __init__(self, pk_cols=None, dedupes=False):
        self.pk_cols: list = pk_cols
        self.dedupes = dedupes

    def __str__(self):
        return json.dumps(self, cls=ConfigEncoder)

source_config = {
    InputData.Primary_Person.value: {
        'pk_cols': ["CRASH_ID", "UNIT_NBR", "PRSN_NBR"]
    },
    InputData.Units.value: {
        'pk_cols': ["CRASH_ID", "UNIT_NBR"],
        'dedupes': True
    }
}

final_config = {**source_config}

def get_source_config(source_file):
    try:
        print(f"Getting configuration for source_file: {source_file}")
        config = final_config[source_file]
        final_source_config = SourceConfig(**config)
        print(f"Config for source_file: {source_file}: \n {final_source_config}")
        return final_source_config
    except Exception as ex:
        print(f"Unable to load config for source: {source_file}, due to {ex}")
        return {}