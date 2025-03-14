# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from json import dumps
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot.enums import MODEL_REPLACEMENT_REASON

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator
from datarobot_provider.operators.datarobot import DATAROBOT_MAX_WAIT
from datarobot_provider.operators.datarobot import DATETIME_FORMAT

if TYPE_CHECKING:
    from datarobot.models.deployment.deployment import ModelDict


def _serialize_drift(drift_obj, date_format=DATETIME_FORMAT):
    drift_dict = drift_obj.__dict__.copy()
    drift_dict["period"] = {
        "start": drift_obj.period["start"].strftime(date_format),
        "end": drift_obj.period["end"].strftime(date_format),
    }
    return drift_dict


class DeployModelMixin:
    def deploy_model(
        self, model_id: str, label: str, description: Optional[str] = None
    ) -> dr.Deployment:
        """Deploys the provided model to production."""
        self.log.info(f"Deploying model_id={model_id} with label={label}")  # type: ignore[attr-defined]
        prediction_server = dr.PredictionServer.list()[0]
        deployment = dr.Deployment.create_from_learning_model(
            model_id, label, description, prediction_server.id
        )
        self.log.info(f"Model deployed: deployment_id={deployment.id}")  # type: ignore[attr-defined]
        self.log.info("Enabling tracking for target drift and feature drift")  # type: ignore[attr-defined]
        deployment.update_drift_tracking_settings(
            target_drift_enabled=True, feature_drift_enabled=True, max_wait=DATAROBOT_MAX_WAIT
        )
        return deployment


class DeployModelOperator(BaseDatarobotOperator, DeployModelMixin):
    """
    Deploys the specified model to production.

    :param model_id: ID of the DataRobot model to deploy
    :type model_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot deployment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["model_id"]

    def __init__(
        self,
        *,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = model_id

    def execute(self, context: Context) -> str:
        # Deploy the model
        deployment = self.deploy_model(
            self.model_id,
            context["params"]["deployment_label"],
            context["params"].get("deployment_description"),
        )
        return deployment.id


class DeployRecommendedModelOperator(BaseDatarobotOperator, DeployModelMixin):
    """
    Deploys a recommended model to production.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot deployment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id"]

    def __init__(
        self,
        *,
        project_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id

    def deploy_recommended_model(
        self, project_id: str, label: str, description: Optional[str] = None
    ) -> dr.Deployment:
        """Deploys the recommended model to production."""
        self.log.info(f"Retrieving recommended model for project_id={project_id}")
        project: dr.Project = dr.Project.get(project_id)
        model = project.recommended_model()
        if model is None:
            raise AirflowFailException(f"No recommended model found for project_id={project_id}")
        return self.deploy_model(model.id, label, description)

    def execute(self, context: Context) -> str:
        # Deploy the recommended model
        deployment = self.deploy_recommended_model(
            self.project_id,
            context["params"]["deployment_label"],
            context["params"].get("deployment_description"),
        )
        return deployment.id


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

    def validate(self) -> None:
        if not self.deployment_id:
            raise ValueError("Invalid or missing `deployment_id` value")

    def execute(self, context: Context) -> Optional[str]:
        self.log.info(f"Getting Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        return deployment.status


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
    template_fields: Sequence[str] = ["deployment_id", "new_registered_model_version_id", "reason"]

    def __init__(
        self,
        *,
        deployment_id: str,
        new_registered_model_version_id: str,
        reason: str = MODEL_REPLACEMENT_REASON.OTHER,
        max_wait_sec: int = 600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.new_registered_model_version_id = new_registered_model_version_id
        self.reason = reason
        self.max_wait_sec = max_wait_sec

    def validate(self) -> None:
        if self.deployment_id is None:
            raise AirflowFailException("deployment_id must be provided")

        if self.new_registered_model_version_id is None:
            raise AirflowFailException("new_registered_model_version_id must be provided")

    def execute(self, context: Context) -> None:
        self.log.info(f"Getting model_id for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)
        self.log.info(
            f"Validating replacement model new_registered_model_version_id={self.new_registered_model_version_id} "
            f"for deployment_id={self.deployment_id}"
        )
        check_result, check_message, status_list = deployment.validate_replacement_model(
            new_registered_model_version_id=self.new_registered_model_version_id
        )
        self.log.info(f"Validation result: {check_result}, message: {check_message}")
        self.log.info("Validation result details:\n%s", dumps(status_list, indent=2))
        if check_result == "failing":
            raise AirflowFailException(check_message)
        self.log.info(
            f"Trying to replace a model for deployment_id={self.deployment_id} "
            f"to new_registered_model_version_id={self.new_registered_model_version_id}"
        )
        deployment.perform_model_replace(
            new_registered_model_version_id=self.new_registered_model_version_id,
            reason=self.reason,
            max_wait=self.max_wait_sec,
        )
        self.log.info(
            f"Model for deployment_id={self.deployment_id} replaced "
            f"to new_registered_model_version_id={self.new_registered_model_version_id}"
        )


class ScorePredictionsOperator(BaseDatarobotOperator):
    """
    Creates a batch prediction job in DataRobot, scores the data and saves prediction to the output.
    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param intake_datastore_id: DataRobot DataStore ID for jdbc source connection
    :type intake_datastore_id: str
    :param intake_credential_id: DataRobot Credentials ID for source connection
    :type intake_credential_id: str
    :param output_datastore_id: DataRobot DataStore ID for jdbc destination connection
    :type output_datastore_id: str
    :param output_credential_id: DataRobot Credentials ID for destination connection
    :type output_credential_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Batch predictions job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "deployment_id",
        "intake_datastore_id",
        "intake_credential_id",
        "output_datastore_id",
        "output_credential_id",
        "score_settings",
    ]

    def __init__(
        self,
        *,
        deployment_id: Optional[str] = None,
        intake_datastore_id: Optional[str] = None,
        intake_credential_id: Optional[str] = None,
        output_datastore_id: Optional[str] = None,
        output_credential_id: Optional[str] = None,
        score_settings: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.intake_datastore_id = intake_datastore_id
        self.intake_credential_id = intake_credential_id
        self.output_datastore_id = output_datastore_id
        self.output_credential_id = output_credential_id
        self.score_settings = score_settings

    def execute(self, context: Context) -> str:
        if self.score_settings is None:
            self.score_settings = context["params"]["score_settings"]

        # in case of deployment_id was not set from operator argument:
        if self.deployment_id is None:
            self.deployment_id = self.score_settings["deployment_id"]

        if self.intake_credential_id is not None:
            self.score_settings["intake_settings"]["credential_id"] = self.intake_credential_id
        if self.output_credential_id is not None:
            self.score_settings["output_settings"]["credential_id"] = self.output_credential_id

        intake_settings = self.score_settings.get("intake_settings", dict())
        output_settings = self.score_settings.get("output_settings", dict())

        intake_type = intake_settings.get("type")
        output_type = output_settings.get("type")

        # in case of JDBC intake from operator argument:
        if intake_type == "jdbc" and self.intake_datastore_id is not None:
            self.score_settings["intake_settings"]["data_store_id"] = self.intake_datastore_id

        # in case of JDBC output from operator argument:
        if output_type == "jdbc" and self.output_datastore_id is not None:
            self.score_settings["output_settings"]["data_store_id"] = self.output_datastore_id

        # Score data
        self.log.info(
            f"Scoring predictions against deployment_id={self.deployment_id} "
            f"with settings: {self.score_settings}"
        )

        # BatchPredictionJob.score() method in the Python SDK expects a DataRobot dataset instance
        if intake_type == "dataset":
            dataset_id = intake_settings.get("dataset_id")
            if not dataset_id:
                raise ValueError(
                    "Invalid or missing `dataset_id` value for the `dataset` intake type."
                )
            dataset = dr.Dataset.get(dataset_id)
            intake_settings["dataset"] = dataset

            # We no longer need the ID
            del intake_settings["dataset_id"]

        job = dr.BatchPredictionJob.score(self.deployment_id, **self.score_settings)
        self.log.info(f"Batch Prediction submitted, job.id={job.id}")
        return job.id


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

    def validate(self) -> None:
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


class GetFeatureDriftOperator(BaseDatarobotOperator):
    """
    Gets feature drift measurements from a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Drift stats for a Deployment's features
    :rtype: list[dict]
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

    def execute(self, context: Context) -> list[dict]:
        self.log.info(f"Getting feature drift for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        feature_drift_params = context["params"].get("feature_drift", {})
        drift = deployment.get_feature_drift(**feature_drift_params)
        return [_serialize_drift(feature) for feature in drift]


class GetTargetDriftOperator(BaseDatarobotOperator):
    """
    Gets target drift measurements from a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Drift stats for a Deployment's target
    :rtype: dict
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

    def execute(self, context: Context) -> dict[str, Any]:
        self.log.info(f"Getting target drift for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        target_drift_params = context["params"].get("target_drift", {})
        drift = deployment.get_target_drift(**target_drift_params)
        return _serialize_drift(drift)


class DeployRegisteredModelOperator(BaseDatarobotOperator):
    """
    Create a deployment from a registered model version using DataRobot's API.

    This operator creates a deployment for a registered model version by calling
    DataRobot's `Deployment.create_from_registered_model_version()` method. It allows
    optional extra parameters to be passed to the DataRobot client call.

    Args:
        model_package_id (str): The registered model version ID to deploy.
        deployment_label (str): The label or name to assign to the deployment.
        extra_params (dict, optional): A dictionary of additional parameters to pass
            to the DataRobot deployment creation API.
        kwargs (dict): Additional keyword arguments passed to the BaseDatarobotOperator.
    """

    template_fields: Sequence[str] = ["model_package_id", "deployment_label", "extra_params"]

    def __init__(
        self,
        *,
        model_package_id: str,
        deployment_label: str,
        extra_params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        self.extra_params = extra_params or {}
        super().__init__(**kwargs)
        self.model_package_id = model_package_id
        self.deployment_label = deployment_label

    def validate(self) -> None:
        if not self.model_package_id:
            raise ValueError("model_package_id must be provided.")
        if not self.deployment_label:
            raise ValueError("label must be provided.")

    def execute(self, context: Context) -> str:
        self.log.info(
            "Creating deployment from registered model version: %s", self.model_package_id
        )

        # Create deployment from registered model.
        deployment = dr.Deployment.create_from_registered_model_version(
            model_package_id=self.model_package_id, label=self.deployment_label, **self.extra_params
        )

        self.log.info("Deployment created with ID: %s", deployment.id)
        return deployment.id
