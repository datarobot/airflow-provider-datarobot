from datarobot import ModelJob

from datarobot_provider.operators.base_datarobot_operator import DatarobotFunctionOperator


class ModelJobGetOperator(DatarobotFunctionOperator):
    function = ModelJob.get
