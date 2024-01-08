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
    },
    Analysis.Analysis_2.value: {
        'source_data': [InputData.Units.value]
    },
    Analysis.Analysis_3.value: {
        'source_data': [InputData.Units.value, InputData.Primary_Person.value]
    },
    Analysis.Analysis_4.value: {
        'source_data': [InputData.Units.value, InputData.Primary_Person.value]
    },
    Analysis.Analysis_5.value: {
        'source_data': [InputData.Units.value, InputData.Primary_Person.value]
    },
    Analysis.Analysis_6.value: {
        'source_data': [InputData.Units.value, InputData.Primary_Person.value]
    },
    Analysis.Analysis_7.value: {
        'source_data': [InputData.Units.value, InputData.Primary_Person.value]
    },
    Analysis.Analysis_8.value: {
        'source_data': [InputData.Units.value, InputData.Primary_Person.value]
    },
    Analysis.Analysis_9.value: {
        'source_data': [InputData.Units.value, InputData.Damages.value]
    },
    Analysis.Analysis_10.value: {
        'source_data': [InputData.Units.value, InputData.Charges.value, InputData.Primary_Person.value]
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