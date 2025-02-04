# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot.enums import MODEL_REPLACEMENT_REASON

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator

if TYPE_CHECKING:
    from datarobot.models.deployment.deployment import ModelDict


class GetDeploymentModelOperator(BaseDatarobotOperator):
    """
    Gets current model info from a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: model info from a Deployment
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]

    def __init__(
        self,
        *,
        deployment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id

    def execute(self, context: Context) -> Optional["ModelDict"]:
        self.log.info(f"Getting model_id for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        return deployment.model


class ReplaceModelOperator(BaseDatarobotOperator):
    """
    Replaces the current model for a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param new_model_id: The id of the new model to use. If replacing the deployment's model with a
            CustomInferenceModel, a specific CustomModelVersion ID must be used.
    :type new_model_id: str
    :param reason: str
            The reason for the model replacement. Must be one of 'ACCURACY', 'DATA_DRIFT', 'ERRORS',
            'SCHEDULED_REFRESH', 'SCORING_SPEED', or 'OTHER'. This value will be stored in the model
            history to keep track of why a model was replaced
    :type reason: str, optional
    :param max_wait_sec: The maximum time to wait for
            model replacement job to complete before erroring,
            defaults to 600 seconds
    :type max_wait_sec: int, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id", "new_model_id", "reason"]

    def __init__(
        self,
        *,
        deployment_id: str,
        new_model_id: str,
        reason: str = MODEL_REPLACEMENT_REASON.OTHER,
        max_wait_sec: int = 600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.new_model_id = new_model_id
        self.reason = reason
        self.max_wait_sec = max_wait_sec

    def validate(self):
        if self.deployment_id is None:
            raise AirflowFailException("deployment_id must be provided")

        if self.new_model_id is None:
            raise AirflowFailException("new_model_id must be provided")

    def execute(self, context: Context) -> None:
        self.log.info(f"Getting model_id for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)
        self.log.info(
            f"Validating replacement model new_model_id={self.new_model_id} for deployment_id={self.deployment_id}"
        )
        check_result, check_message, status_list = deployment.validate_replacement_model(
            new_model_id=self.new_model_id
        )
        self.log.info(f"Validation result: {check_result}, message: {check_message}")
        self.log.info(f"Validation result details: {status_list}")
        if check_result == "failing":
            raise AirflowFailException(check_message)
        self.log.info(
            f"Trying to replace a model for deployment_id={self.deployment_id} to new_model_id={self.new_model_id}"
        )
        deployment.replace_model(
            new_model_id=self.new_model_id, reason=self.reason, max_wait=self.max_wait_sec
        )
        self.log.info(
            f"Model for deployment_id={self.deployment_id} replaced to new_model_id={self.new_model_id}"
        )


class ActivateDeploymentOperator(BaseDatarobotOperator):
    """
    Activate or deactivate a Deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param activate: if set to True - activate deployment, if set to False - deactivate deployment
    :type activate: boolean
    :param max_wait_sec: The maximum time in seconds to wait for deployment activation/deactivation to complete
    :type max_wait_sec: int
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Deployment status (active/inactive)
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id", "activate"]

    def __init__(
        self,
        *,
        deployment_id: str,
        activate: bool = True,
        max_wait_sec: int = 600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.activate = activate
        self.max_wait_sec = max_wait_sec

    def validate(self):
        if self.deployment_id is None:
            raise ValueError("Invalid or missing `deployment_id` value")

    def execute(self, context: Context) -> Optional[str]:
        self.log.info(f"Getting Deployment with deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        if self.activate:
            deployment.activate(max_wait=self.max_wait_sec)
        else:
            deployment.deactivate(max_wait=self.max_wait_sec)
        return deployment.status


class GetDeploymentStatusOperator(BaseDatarobotOperator):
    """
    Get a Deployment status (active/inactive).

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Deployment status (active/inactive)
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]

    def __init__(
        self,
        *,
        deployment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id

    def validate(self):
        if not self.deployment_id:
            raise ValueError("Invalid or missing `deployment_id` value")

    def execute(self, context: Context) -> Optional[str]:
        self.log.info(f"Getting Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        return deployment.status
