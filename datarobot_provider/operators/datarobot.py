# Copyright 2022 DataRobot, Inc. and its affiliates.
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
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook

DATAROBOT_MAX_WAIT = 3600
DATAROBOT_AUTOPILOT_TIMEOUT = 86400
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%s"


class CreateProjectOperator(BaseOperator):
    """
    Creates DataRobot project.
    :param dataset_id: DataRobot AI Catalog dataset ID
    :type dataset_id: str, optional
    :param dataset_version_id: DataRobot AI Catalog dataset version ID
    :type dataset_version_id: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot project ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["dataset_id", "dataset_version_id", "credential_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        dataset_version_id: Optional[str] = None,
        credential_id: Optional[str] = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.dataset_version_id = dataset_version_id
        self.datarobot_conn_id = datarobot_conn_id
        self.credential_id = credential_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> Optional[str]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Create DataRobot project
        self.log.info("Creating DataRobot project")

        if self.dataset_id is None and "training_data" in context["params"]:
            # training_data may be a pre-signed URL to a file on S3 or a path to a local file
            project: dr.Project = dr.Project.create(
                context["params"]["training_data"], context["params"]["project_name"]
            )
            self.log.info(f"Project created: project_id={project.id} from local file")
            project.unsupervised_mode = context["params"].get("unsupervised_mode")
            project.use_feature_discovery = context["params"].get("use_feature_discovery")
            project.unlock_holdout()
            return project.id

        elif self.dataset_id is not None or "training_dataset_id" in context["params"]:
            # training_dataset_id may be provided via params
            # or dataset_id should be returned from previous operator
            training_dataset_id = (
                self.dataset_id
                if self.dataset_id is not None
                else context["params"]["training_dataset_id"]
            )

            project = dr.Project.create_from_dataset(
                dataset_id=training_dataset_id,
                dataset_version_id=self.dataset_version_id,
                credential_id=self.credential_id,
                project_name=context["params"]["project_name"],
            )
            # Some weird problem with mypy: it passes here locally, but fails in CI
            self.log.info(
                f"Project created: project_id={project.id} from dataset: dataset_id={training_dataset_id}"  # type: ignore[attr-defined, unused-ignore]
            )
            return project.id  # type: ignore[attr-defined, unused-ignore]

        else:
            raise AirflowFailException(
                "For Project creation training_data or training_dataset_id must be provided"
            )


class TrainModelsOperator(BaseOperator):
    """
    Triggers DataRobot Autopilot to train models.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        project_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> None:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Train models
        project = dr.Project.get(self.project_id)
        if project.target:
            self.log.info(f"Models are already trained for project_id={project.id}")
        else:
            self.log.info(
                f"Starting DataRobot Autopilot for project_id={project.id} "
                f"with settings={context['params']['autopilot_settings']}"
            )
            project.set_target(**context["params"]["autopilot_settings"])


class DeployModelMixin:
    def deploy_model(
        self, model_id: str, label: str, description: Optional[str] = None
    ) -> dr.Deployment:
        """Deploys the provided model to production."""
        self.log.info(f"Deploying model_id={model_id} with label={label}")  # type: ignore
        prediction_server = dr.PredictionServer.list()[0]
        deployment = dr.Deployment.create_from_learning_model(
            model_id, label, description, prediction_server.id
        )
        self.log.info(f"Model deployed: deployment_id={deployment.id}")  # type: ignore
        self.log.info("Enabling tracking for target drift and feature drift")  # type: ignore
        deployment.update_drift_tracking_settings(
            target_drift_enabled=True, feature_drift_enabled=True, max_wait=DATAROBOT_MAX_WAIT
        )
        return deployment


class DeployModelOperator(BaseOperator, DeployModelMixin):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        model_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = model_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Deploy the model
        deployment = self.deploy_model(
            self.model_id,
            context["params"]["deployment_label"],
            context["params"].get("deployment_description"),
        )
        return deployment.id


class DeployRecommendedModelOperator(BaseOperator, DeployModelMixin):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        project_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

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
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Deploy the recommended model
        deployment = self.deploy_recommended_model(
            self.project_id,
            context["params"]["deployment_label"],
            context["params"].get("deployment_description"),
        )
        return deployment.id


class ScorePredictionsOperator(BaseOperator):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: Optional[str] = None,
        intake_datastore_id: Optional[str] = None,
        intake_credential_id: Optional[str] = None,
        output_datastore_id: Optional[str] = None,
        output_credential_id: Optional[str] = None,
        score_settings: Optional[dict] = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.intake_datastore_id = intake_datastore_id
        self.intake_credential_id = intake_credential_id
        self.output_datastore_id = output_datastore_id
        self.output_credential_id = output_credential_id
        self.score_settings = score_settings
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

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


class GetTargetDriftOperator(BaseOperator):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> dict[str, Any]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Getting target drift for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        target_drift_params = context["params"].get("target_drift", {})
        drift = deployment.get_target_drift(**target_drift_params)
        return _serialize_drift(drift)


class GetFeatureDriftOperator(BaseOperator):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> list[dict]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Getting feature drift for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        feature_drift_params = context["params"].get("feature_drift", {})
        drift = deployment.get_feature_drift(**feature_drift_params)
        return [_serialize_drift(feature) for feature in drift]


def _serialize_drift(drift_obj, date_format=DATETIME_FORMAT):
    drift_dict = drift_obj.__dict__.copy()
    drift_dict["period"] = {
        "start": drift_obj.period["start"].strftime(date_format),
        "end": drift_obj.period["end"].strftime(date_format),
    }
    return drift_dict
