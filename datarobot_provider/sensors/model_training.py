# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from typing import Union

from airflow.sensors.base import PokeReturnValue
from airflow.utils.context import Context
from datarobot import ModelJob

from datarobot_provider.sensors.model_insights import DataRobotJobSensor


class ModelTrainingJobSensor(DataRobotJobSensor):
    """
    Checks whether DataRobot Model Training Job is complete.

    Args:
        project_id (str): DataRobot project ID.
        job_id (str): DataRobot Job ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        bool | PokeReturnValue: False if not yet completed, PokeReturnValue(True, trained_model.id) if model training completed.
    """

    def get_job_result(self, context: Context) -> Union[bool, PokeReturnValue]:
        trained_model = ModelJob.get_model(self.project_id, self.job_id)
        return PokeReturnValue(True, trained_model.id)
