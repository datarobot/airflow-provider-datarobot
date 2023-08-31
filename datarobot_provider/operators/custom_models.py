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
from datarobot.models.execution_environment import RequiredMetadataKey

from datarobot_provider.hooks.datarobot import DataRobotHook

DEFAULT_MAX_WAIT_SEC = 3600  # 1 hour timeout by default


class CreateExecutionEnvironmentOperator(BaseOperator):
    """
    Create an execution environment.
    :param name: execution environment name
    :type name: str
    :param description: execution environment description
    :type description: str, optional
    :param programming_language: programming language of the environment to be created.
        Can be "python", "r", "java" or "other". Default value - "other"
    :type programming_language: str, optional
    :return: execution environment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "name",
        "description",
        "programming_language",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        name: str = None,
        description: str = None,
        programming_language: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.programming_language = programming_language
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        required_metadata_keys = context["params"].get("required_metadata_keys", None)
        metadata_keys = []
        if required_metadata_keys:
            metadata_keys = [
                RequiredMetadataKey(field_name=key["field_name"], display_name=key["display_name"])
                for key in required_metadata_keys
            ]

        execution_environment_name = (
            context["params"].get("execution_environment_name", None)
            if self.name is None
            else self.name
        )
        execution_environment_description = (
            context["params"].get("execution_environment_description", None)
            if self.description is None
            else self.description
        )
        programming_language = (
            context["params"].get("programming_language", None)
            if self.programming_language is None
            else self.programming_language
        )

        execution_environment = dr.ExecutionEnvironment.create(
            name=execution_environment_name,
            description=execution_environment_description,
            programming_language=programming_language,
            required_metadata_keys=metadata_keys,
        )

        self.log.info(
            f"Execution environment created, execution_environment_id={execution_environment.id}"
        )

        return execution_environment.id


class CreateExecutionEnvironmentVersionOperator(BaseOperator):
    """
    Create an execution environment version.
    :param execution_environment_id: the id of the execution environment
    :type execution_environment_id: str
    :param docker_context_path: the path to a docker context archive or folder
    :type docker_context_path: str
    :param environment_version_label: short human readable string to label the version.
    :type environment_version_label: str, optional
    :param environment_version_description: execution environment version description
    :type environment_version_description: str, optional
    :param max_wait: max time in seconds to wait for a final build status ("success" or "failed").
        If set to None - method will return without waiting.
    :type max_wait: int, optional
    :return: created execution environment version ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "execution_environment_id",
        "docker_context_path",
        "environment_version_label",
        "environment_version_description",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        execution_environment_id: str,
        docker_context_path: str = None,
        environment_version_label: str = None,
        environment_version_description: str = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.execution_environment_id = execution_environment_id
        self.docker_context_path = docker_context_path
        self.environment_version_label = environment_version_label
        self.environment_version_description = environment_version_description
        self.max_wait_sec = max_wait_sec
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        docker_context_path = (
            context["params"].get("docker_context_path", None)
            if self.docker_context_path is None
            else self.docker_context_path
        )
        version_label = (
            context["params"].get("environment_version_label", None)
            if self.environment_version_label is None
            else self.environment_version_label
        )
        version_description = (
            context["params"].get("environment_version_description", None)
            if self.environment_version_description is None
            else self.environment_version_description
        )

        environment_version = dr.ExecutionEnvironmentVersion.create(
            execution_environment_id=self.execution_environment_id,
            docker_context_path=docker_context_path,
            label=version_label,
            description=version_description,
            max_wait=self.max_wait_sec,
        )

        self.log.info(
            f"Execution environment version created, environment_version_id={environment_version.id}"
        )

        return environment_version.id


class CreateCustomInferenceModelOperator(BaseOperator):
    """
    Create a custom inference model.
    :param name: Name of the custom model.
    :type name: str
    :param description: Description of the custom model.
    :type description: str
    :return: created custom model ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "name",
        "description",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        name: str = None,
        description: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        custom_model_name = (
            context["params"].get("custom_model_name", None) if self.name is None else self.name
        )

        if custom_model_name is None:
            raise ValueError(
                "Custom model name is required attribute for CreateCustomInferenceModelOperator"
            )

        custom_model_description = (
            context["params"].get("custom_model_description", None)
            if self.description is None
            else self.description
        )

        target_type = context["params"].get("target_type", None)

        if target_type is None:
            raise ValueError(
                "target_type is required attribute for CreateCustomInferenceModelOperator"
            )

        custom_model = dr.CustomInferenceModel.create(
            name=custom_model_name,
            description=custom_model_description,
            target_type=context["params"].get("target_type", None),
            target_name=context["params"].get("target_name", None),
            language=context["params"].get("programming_language", None),
            positive_class_label=context["params"].get("positive_class_label", None),
            negative_class_label=context["params"].get("negative_class_label", None),
            prediction_threshold=context["params"].get("prediction_threshold", None),
            class_labels=context["params"].get("class_labels", None),
            class_labels_file=context["params"].get("class_labels_file", None),
            network_egress_policy=context["params"].get("network_egress_policy", None),
            maximum_memory=context["params"].get("maximum_memory", None),
            replicas=context["params"].get("replicas", None),
            is_training_data_for_versions_permanently_enabled=context["params"].get(
                "is_training_data_for_versions_permanently_enabled", None
            ),
        )

        self.log.info(f"Custom Inference Model created, custom_model_id={custom_model.id}")

        return custom_model.id


class CreateCustomModelVersionOperator(BaseOperator):
    """
    Create a custom model version without files from previous versions.

    :param custom_model_id: The ID of the custom model.
    :type custom_model_id: str
    :param base_environment_id: The ID of the base environment to use with the custom model version.
    :type base_environment_id: str
    :param training_dataset_id: The ID of the training dataset to assign to the custom model.
    :type training_dataset_id: str, optional
    :param holdout_dataset_id: The ID of the holdout dataset to assign to the custom model.
            Can only be assigned for unstructured models.
    :type holdout_dataset_id: str, optional
    :param custom_model_folder: The ID of the holdout dataset to assign to the custom model.
            Can only be assigned for unstructured models.
    :type custom_model_folder: str, optional
    :param max_wait:  Max time to wait for training data assignment.
    :type max_wait: int, optional
    :return: created custom model version ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "custom_model_id",
        "base_environment_id",
        "training_dataset_id",
        "holdout_dataset_id",
        "custom_model_folder",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        custom_model_id: str,
        base_environment_id: str,
        training_dataset_id: str = None,
        holdout_dataset_id: str = None,
        custom_model_folder: str = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_model_id = custom_model_id
        self.base_environment_id = base_environment_id
        self.training_dataset_id = training_dataset_id
        self.holdout_dataset_id = holdout_dataset_id
        self.custom_model_folder = custom_model_folder
        self.max_wait_sec = max_wait_sec
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        folder_path = (
            context["params"].get("custom_model_folder", None)
            if self.custom_model_folder is None
            else self.custom_model_folder
        )

        if self.custom_model_id is None:
            raise ValueError(
                "custom_model_id is required attribute for CreateCustomModelVersionOperator"
            )

        if self.base_environment_id is None:
            raise ValueError(
                "base_environment_id is required attribute for CreateCustomModelVersionOperator"
            )

        custom_model_version = dr.CustomModelVersion.create_clean(
            custom_model_id=self.custom_model_id,
            training_dataset_id=self.training_dataset_id,
            base_environment_id=self.base_environment_id,
            holdout_dataset_id=self.holdout_dataset_id,
            folder_path=folder_path,
            is_major_update=context["params"].get("is_major_update", None),
            files=context["params"].get("files", None),
            network_egress_policy=context["params"].get("network_egress_policy", None),
            maximum_memory=context["params"].get("maximum_memory", None),
            replicas=context["params"].get("replicas", None),
            required_metadata_values=context["params"].get("required_metadata_values", None),
            partition_column=context["params"].get("partition_column", None),
            keep_training_holdout_data=context["params"].get("keep_training_holdout_data", None),
            max_wait=self.max_wait_sec,
        )

        self.log.info(
            f"Custom Model Version created, custom_model_version_id={custom_model_version.id}"
        )

        return custom_model_version.id
