from datarobot import ModelJob

from datarobot_provider.operators.base_datarobot_operator import DatarobotMethodOperator


class ModelJobGetOperator(DatarobotMethodOperator):
    method = ModelJob.get
