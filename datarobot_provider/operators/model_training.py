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
from datarobot import BatchMonitoringJob, Blueprint

from datarobot_provider.hooks.datarobot import DataRobotHook


class TrainModelOperator(BaseOperator):
    """
    Submit a job to the queue to train a model.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param blueprint_id: DataRobot blueprint ID
    :type blueprint_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: model training job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "project_id",
        "blueprint_id",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        project_id: str = None,
        blueprint_id: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.blueprint_id = blueprint_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        project = dr.Project.get(self.project_id)
        blueprint = Blueprint.get(project.id, self.blueprint_id)

        job_id = project.train(blueprint, training_row_count=project.max_train_rows)

        self.log.info(f"Monitoring Job submitted job_id={job_id}")

        return job_id
