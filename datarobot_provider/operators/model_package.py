# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot.utils.waiters import wait_for_async_resolution

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator

DEFAULT_MAX_WAIT_SEC = 600


class CreateExternalModelPackageOperator(BaseDatarobotOperator):
    """
    Create an external model package in DataRobot MLOps from JSON configuration

    :param model_package_json: A JSON object of external model parameters.
    :type model_package_json: dict
    :return: A model package ID of newly created ModelPackage.
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "model_package_json",
    ]

    def __init__(
        self,
        *,
        model_package_json: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_package_json = model_package_json

    # Utility method to support old Model Registry API
    def create_model_package_from_json(self) -> str:
        response = dr.client.get_client().post(
            "modelPackages/fromJSON/", data=self.model_package_json
        )
        if response.status_code == 201:
            response_json = response.json()
            model_package_id = response_json["id"]
            return model_package_id
        else:
            e_msg = "Server unexpectedly returned status code {}"
            raise AirflowFailException(e_msg.format(response.status_code))

    def execute(self, context: Context) -> str:
        if self.model_package_json is None:
            # If model_package_json not provided, trying to get it from DAG params:
            self.model_package_json = context["params"].get("model_package_json")

        if self.model_package_json is None:
            raise ValueError("model_package_json is required.")

        model_package_id = self.create_model_package_from_json()

        self.log.info(f"External Model Package Created, model_package_id={model_package_id}")

        return model_package_id


class DeployModelPackageOperator(BaseDatarobotOperator):
    """
    Create a deployment from a DataRobot model package.
    :deployment_name: A human readable label of the deployment.
    :deployment_name: str
    :model_package_id: The ID of the DataRobot model package to deploy.
    :model_package_id: str
    :default_prediction_server_id: an identifier of a prediction server to be used as the default prediction server
        When working with prediction environments, default prediction server Id should not be provided
    :default_prediction_server_id: str, optional
    :prediction_environment_id:  An identifier of a prediction environment to be used for model deployment.
    :prediction_environment_id: str, optional
    :description: A human readable description of the deployment.
    :description: str, optional
    :importance: Deployment importance level.
    :importance: str, optional
    :user_provided_id: A user-provided unique ID associated with a deployment definition in a remote git repository.
    :user_provided_id: str, optional
    :additional_metadata: A Key/Value pair dict, with additional metadata
    :additional_metadata: dict[str, str], optional
    :max_wait: The amount of seconds to wait for successful resolution of a deployment creation job.
        Deployment supports making predictions only after a deployment creating job has successfully finished.
    :max_wait: int, optional
    :return: The created deployment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "deployment_name",
        "model_package_id",
        "default_prediction_server_id",
        "prediction_environment_id",
        "description",
        "importance",
        "user_provided_id",
        "additional_metadata",
    ]

    def __init__(
        self,
        *,
        deployment_name: str,
        model_package_id: str,
        default_prediction_server_id: Optional[str] = None,
        prediction_environment_id: Optional[str] = None,
        description: Optional[str] = None,
        importance: Optional[str] = None,
        user_provided_id: Optional[str] = None,
        additional_metadata: Optional[dict[str, str]] = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_name = deployment_name
        self.model_package_id = model_package_id
        self.default_prediction_server_id = default_prediction_server_id
        self.prediction_environment_id = prediction_environment_id
        self.description = description
        self.importance = importance
        self.user_provided_id = user_provided_id
        self.additional_metadata = additional_metadata
        self.max_wait_sec = max_wait_sec

    # Utility method to support creating Deployment from Model Package
    # Will be refactored after release 3.3 of DataRobot public python client
    @classmethod
    def _create_from_model_package(
        cls,
        model_package_id: str,
        deployment_name: str,
        description: Optional[str] = None,
        default_prediction_server_id: Optional[str] = None,
        prediction_environment_id: Optional[str] = None,
        importance: Optional[str] = None,
        user_provided_id: Optional[str] = None,
        additional_metadata: Optional[dict[str, str]] = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
    ) -> str:
        deployment_payload: dict[str, Any] = {
            "model_package_id": model_package_id,
            "label": deployment_name,
            "description": description,
        }
        if default_prediction_server_id and prediction_environment_id:
            raise ValueError(
                "When working with prediction environments, default prediction server Id should not be provided"
            )
        elif default_prediction_server_id and prediction_environment_id is None:
            deployment_payload["default_prediction_server_id"] = default_prediction_server_id
        elif prediction_environment_id and default_prediction_server_id is None:
            deployment_payload["prediction_environment_id"] = prediction_environment_id

        if importance:
            deployment_payload["importance"] = importance
        if user_provided_id:
            deployment_payload["user_provided_id"] = user_provided_id
        if additional_metadata:
            deployment_payload["additional_metadata"] = additional_metadata
        dr_client = dr.client.get_client()

        response = dr_client.post("deployments/fromModelPackage/", data=deployment_payload)

        if response.status_code == 202 and "Location" in response.headers:
            wait_for_async_resolution(dr_client, response.headers["Location"], max_wait_sec)
            deployment_id = response.json()["id"]
            return deployment_id
        else:
            e_msg = "Server unexpectedly returned status code {}"
            raise AirflowFailException(e_msg.format(response.status_code))

    def validate(self) -> None:
        if self.deployment_name is None:
            raise ValueError("deployment_name is required.")

        if self.model_package_id is None:
            raise ValueError("model_package_id is required.")

    def execute(self, context: Context) -> str:
        if self.additional_metadata is None:
            # If additional_metadata not provided, trying to get it from DAG params:
            self.additional_metadata = context["params"].get("additional_metadata")

        deployment_id = self._create_from_model_package(
            model_package_id=self.model_package_id,
            deployment_name=self.deployment_name,
            description=self.description,
            default_prediction_server_id=self.default_prediction_server_id,
            prediction_environment_id=self.prediction_environment_id,
            importance=self.importance,
            user_provided_id=self.user_provided_id,
            additional_metadata=self.additional_metadata,
            max_wait_sec=self.max_wait_sec,
        )

        self.log.info(f"Deployment Created, deployment_id={deployment_id}")

        return deployment_id
