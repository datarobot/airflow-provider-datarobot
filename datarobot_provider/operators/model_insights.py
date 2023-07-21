# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict
from typing import Iterable

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook


class ComputePredictionExplanationsOperator(BaseOperator):
    """
    Creates Predictions Explanation job in DataRobot.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param model_id: DataRobot model ID
    :type model_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Predictions Explanation job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "project_id",
        "model_id",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        project_id: str = None,
        model_id: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.project_id is None:
            raise ValueError("project_id is required to compute predictions explanation.")

        if self.model_id is None:
            raise ValueError("model_id is required to compute predictions explanation.")

        model = dr.models.Model.get(self.project_id, self.model_id)

        job = model.request_feature_impact()

        self.log.info(f"Compute Predictions Explanation Job submitted job_id={job.id}")

        return job.id
