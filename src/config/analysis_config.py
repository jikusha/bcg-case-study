import json

from project_utils import *


class ConfigEncoder(json.JSONEncoder):
    def default(self, o):
        return o.__dict__

class AnalysisConfig:
    def __init__(self, source_data=None):
        self.source_data: list = source_data

    def __str__(self):
        return json.dumps(self, cls=ConfigEncoder)

analysis_config = {
    Analysis.Analysis_1.value: {
        'source_data': [InputData.Primary_Person.value]
    }
}

final_config = {**analysis_config}

def get_analysis_config(analysis_number):
    try:
        print(f"Getting configuration for analysis: {analysis_number}")
        config = final_config[analysis_number]
        final_analysis_config = AnalysisConfig(**config)
        print(f"Config for analysis: {analysis_number}: \n {final_analysis_config}")
        return final_analysis_config
    except KeyError as ex:
        print(f"Unable to load config for analysis: {analysis_number}, as analysis_number is not correct")
        raise Exception(f"Unable to load config for analysis: {analysis_number}, as analysis_number is not correct")
    except Exception as ex:
        print(f"Unable to load config for analysis: {analysis_number}, due to {ex}")
        raise ex