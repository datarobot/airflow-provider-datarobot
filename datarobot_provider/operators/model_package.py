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

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook

DEFAULT_MAX_WAIT_SEC = 600


class CreateExternalModelPackageOperator(BaseOperator):
    """
    Create an external model package in DataRobot MLOps from JSON configuration

    :param model_info: A JSON object of external model parameters.
    :type model_info: dict
    :return: A model package ID of newly created ModelPackage.
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "model_package_json",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        model_package_json: dict = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_package_json = model_package_json
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    # Utility method to support old Model Registry API
    def create_model_package_from_json(self, model_info: Dict[str, Any]) -> str:
        response = dr.client.get_client().post("modelPackages/fromJSON/", data=model_info)
        response_json = response.json()
        model_package_id = response_json["id"]
        self.log.info(f"Created Model Package, id={model_package_id}")
        return model_package_id

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.model_package_json is None:
            # If model_package_json not provided, trying to get it from DAG params:
            self.model_package_json = context["params"].get("model_package_json")

        if self.model_package_json is None:
            raise ValueError("model_package_json is required.")

        model_package_id = self.create_model_package_from_json(self.model_package_json)

        self.log.info(f"External Model Package Created, model_package_id={model_package_id}")

        return model_package_id
