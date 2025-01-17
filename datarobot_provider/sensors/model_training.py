# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict

from airflow.sensors.base import PokeReturnValue
from datarobot import ModelJob

from datarobot_provider.sensors.model_insights import DataRobotJobSensor


class ModelTrainingJobSensor(DataRobotJobSensor):
    """
    Checks whether DataRobot Model Training Job is complete.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param job_id: DataRobot Job ID
    :type job_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: False if not yet completed, PokeReturnValue(True, trained_model.id) if model training completed
    :rtype: bool | PokeReturnValue
    """

    def get_job_result(self, context: Dict[Any, Any]) -> PokeReturnValue:
        trained_model = ModelJob.get_model(self.project_id, self.job_id)
        return PokeReturnValue(True, trained_model.id)
