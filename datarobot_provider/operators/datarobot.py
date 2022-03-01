# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any, Dict, Iterable

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
import datarobot as dr

from datarobot_provider.hooks.datarobot import DataRobotHook

DATAROBOT_MAX_WAIT = 3600
DATAROBOT_AUTOPILOT_TIMEOUT = 86400


class CreateProjectOperator(BaseOperator):
    """
    Creates DataRobot project.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot project ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = []
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Create DataRobot project
        self.log.info("Creating training dataset in DataRobot AI Catalog")
        dataset = dr.Dataset.create_from_url(context["params"]["training_data"])
        self.log.info(f"Created dataset: dataset_id={dataset.id}")
        project = dr.Project.create_from_dataset(dataset.id, project_name=context['params']['project_name'])
        self.log.info(f"Project created: project_id={project.id}")
        project.unsupervised_mode = context['params'].get('unsupervised_mode')
        project.use_feature_discovery = context['params'].get('use_feature_discovery')
        project.unlock_holdout()
        return project.id


class TrainModelsOperator(BaseOperator):
    """
    Triggers DataRobot Autopilot to train models.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = ["project_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

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
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> None:
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
            project.set_target(**context['params']['autopilot_settings'])


class DeployModelMixin:
    def deploy_model(self, model_id: str, label: str, description: str = None) -> dr.Deployment:
        """Deploys the provided model to production."""
        self.log.info(f"Deploying model_id={model_id} with label={label}")  # type: ignore
        prediction_server = dr.PredictionServer.list()[0]
        deployment = dr.Deployment.create_from_learning_model(model_id, label, description, prediction_server.id)
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
    template_fields: Iterable[str] = ["model_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

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
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Deploy the model
        deployment = self.deploy_model(
            self.model_id, context['params']['deployment_label'], context['params'].get('deployment_description'),
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
    template_fields: Iterable[str] = ["project_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

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
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def deploy_recommended_model(self, project_id: str, label: str, description: str = None) -> dr.Deployment:
        """Deploys the recommended model to production."""
        self.log.info(f"Retrieving recommended model for project_id={project_id}")
        project = dr.Project.get(project_id)
        model = project.recommended_model()
        return self.deploy_model(model.id, label, description)

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Deploy the recommended model
        deployment = self.deploy_recommended_model(
            self.project_id, context['params']['deployment_label'], context['params'].get('deployment_description'),
        )
        return deployment.id


class ScorePredictionsOperator(BaseOperator):
    """
    Creates a batch prediction job in DataRobot, scores the data and saves prediction to the output.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Batch predictions job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = ["deployment_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

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
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Score data
        self.log.info(
            f"Scoring predictions against deployment_id={self.deployment_id} "
            f"with settings: {context['params']['score_settings']}"
        )
        job = dr.BatchPredictionJob.score(
            self.deployment_id,
            **context["params"]["score_settings"],
        )
        return job.id
