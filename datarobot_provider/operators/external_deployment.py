# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from datarobot.enums import MODEL_REPLACEMENT_REASON

from datarobot_provider.hooks.datarobot import DataRobotHook

DEFAULT_MAX_WAIT_SEC = 3600


class CreateExternalModelDeploymentOperator(BaseOperator):
    # """
    # Create a deployment from an external model.
    #
    # :param model_package_id: id of the DataRobot external model to deploy
    # :type custom_model_version_id: str
    # :param deployment_name: a human readable label (name) of the deployment
    # :type deployment_name: str
    # :param default_prediction_server_id: an identifier of a prediction server to be used as the default prediction server
    # :type default_prediction_server_id: str, optional
    # :param description: a human readable description of the deployment
    # :type description: str, optional
    # :param importance: deployment importance
    # :type importance: str, optional
    # :param max_wait_sec: seconds to wait for successful resolution of a deployment creation job.
    #     Deployment supports making predictions only after a deployment creating job
    #     has successfully finished
    # :type max_wait_sec: int, optional
    # :return: The created deployment id
    # :rtype: ste
    # """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "external_model_package_id",
        "deployment_name",
        "prediction_server_id",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        model_package_id: str,
        deployment_name: str,
        prediction_server_id: str = None,
        description: str = None,
        importance: str = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_package_id = model_package_id
        self.deployment_name = deployment_name
        self.prediction_server_id = prediction_server_id
        self.description = description
        self.importance = importance
        self.max_wait_sec = max_wait_sec
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.custom_model_version_id is None:
            raise ValueError("Invalid or missing `custom_model_version_id` value")

        if self.deployment_name is None:
            raise ValueError("Invalid or missing `deployment_name` value")

        deployment = dr.Deployment.create_from_model_package(
            custom_model_version_id=self.custom_model_version_id,
            label=self.deployment_name,
            description=self.description,
            default_prediction_server_id=self.prediction_server_id,
            importance=self.importance,
            max_wait=self.max_wait_sec,
        )

        self.log.info(f"Deployment created, deployment_id: {deployment.id}")
        return deployment.id
