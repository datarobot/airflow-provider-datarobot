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
from airflow.utils.context import Context
from datarobot.models.execution_environment import RequiredMetadataKey

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator

DEFAULT_MAX_WAIT_SEC = 3600  # 1 hour timeout by default


class CreateExecutionEnvironmentOperator(BaseDatarobotOperator):
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
    template_fields: Sequence[str] = [
        "name",
        "description",
        "programming_language",
    ]

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        programming_language: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.programming_language = programming_language

    def execute(self, context: Context) -> str:
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


class CreateExecutionEnvironmentVersionOperator(BaseDatarobotOperator):
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
    template_fields: Sequence[str] = [
        "execution_environment_id",
        "docker_context_path",
        "environment_version_label",
        "environment_version_description",
    ]

    def __init__(
        self,
        *,
        execution_environment_id: str,
        docker_context_path: Optional[str] = None,
        environment_version_label: Optional[str] = None,
        environment_version_description: Optional[str] = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.execution_environment_id = execution_environment_id
        self.docker_context_path = docker_context_path
        self.environment_version_label = environment_version_label
        self.environment_version_description = environment_version_description
        self.max_wait_sec = max_wait_sec

    def execute(self, context: Context) -> str:
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


class CreateCustomInferenceModelOperator(BaseDatarobotOperator):
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
    template_fields: Sequence[str] = [
        "name",
        "description",
    ]

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description

    def execute(self, context: Context) -> str:
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


class CreateCustomModelVersionOperator(BaseDatarobotOperator):
    """
    Create a custom model version.

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
    :create_from_previous: if set to True - creates a custom model version containing files from a previous version.
        if set to False - creates a custom model version without files from previous versions.
        value by default is: False.
    :create_from_previous: bool, optional
    :param max_wait:  Max time to wait for training data assignment.
    :type max_wait: int, optional
    :return: created custom model version ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "custom_model_id",
        "base_environment_id",
        "training_dataset_id",
        "holdout_dataset_id",
        "custom_model_folder",
        "create_from_previous",
    ]

    def __init__(
        self,
        *,
        custom_model_id: str,
        base_environment_id: str,
        training_dataset_id: Optional[str] = None,
        holdout_dataset_id: Optional[str] = None,
        custom_model_folder: Optional[str] = None,
        create_from_previous: bool = False,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_model_id = custom_model_id
        self.base_environment_id = base_environment_id
        self.training_dataset_id = training_dataset_id
        self.holdout_dataset_id = holdout_dataset_id
        self.custom_model_folder = custom_model_folder
        self.create_from_previous = create_from_previous
        self.max_wait_sec = max_wait_sec

    def validate(self) -> None:
        if self.custom_model_id is None:
            raise ValueError(
                "custom_model_id is required attribute for CreateCustomModelVersionOperator"
            )

        if self.base_environment_id is None:
            raise ValueError(
                "base_environment_id is required attribute for CreateCustomModelVersionOperator"
            )

    def execute(self, context: Context) -> str:
        folder_path = (
            context["params"].get("custom_model_folder", None)
            if self.custom_model_folder is None
            else self.custom_model_folder
        )

        if self.create_from_previous:
            custom_model_version = dr.CustomModelVersion.create_from_previous(
                custom_model_id=self.custom_model_id,
                training_dataset_id=self.training_dataset_id,
                base_environment_id=self.base_environment_id,
                holdout_dataset_id=self.holdout_dataset_id,
                folder_path=folder_path,
                is_major_update=context["params"].get("is_major_update", None),
                files=context["params"].get("files", None),
                files_to_delete=context["params"].get("files_to_delete", None),
                network_egress_policy=context["params"].get("network_egress_policy", None),
                maximum_memory=context["params"].get("maximum_memory", None),
                replicas=context["params"].get("replicas", None),
                required_metadata_values=context["params"].get("required_metadata_values", None),
                partition_column=context["params"].get("partition_column", None),
                keep_training_holdout_data=context["params"].get(
                    "keep_training_holdout_data", None
                ),
                max_wait=self.max_wait_sec,
            )
        else:
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
                keep_training_holdout_data=context["params"].get(
                    "keep_training_holdout_data", None
                ),
                max_wait=self.max_wait_sec,
            )

        self.log.info(
            f"Custom Model Version created, custom_model_version_id={custom_model_version.id}"
        )

        return custom_model_version.id


class CustomModelTestOperator(BaseDatarobotOperator):
    """
    Create and start a custom model test.

    :param custom_model_id: The ID of the custom model.
    :type custom_model_id: str
    :param custom_model_version_id: The ID of the custom model version.
    :type custom_model_version_id: str
    :param dataset_id: The id of the testing dataset for non-unstructured custom models.
            Ignored and not required for unstructured models.
    :type dataset_id: str, optional
    :param max_wait:  Max time to wait for training data assignment.
    :type max_wait: int, optional
    :return: created custom model test ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["custom_model_id", "custom_model_version_id", "dataset_id"]

    def __init__(
        self,
        *,
        custom_model_id: str,
        custom_model_version_id: str,
        dataset_id: Optional[str] = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_model_id = custom_model_id
        self.custom_model_version_id = custom_model_version_id
        self.dataset_id = dataset_id
        self.max_wait_sec = max_wait_sec

    def validate(self) -> None:
        if self.custom_model_id is None:
            raise ValueError("custom_model_id is required attribute")

        if self.custom_model_version_id is None:
            raise ValueError("custom_model_version_id is required attribute")

    def execute(self, context: Context) -> str:
        # Perform custom model tests
        custom_model_test = dr.CustomModelTest.create(
            custom_model_id=self.custom_model_id,
            custom_model_version_id=self.custom_model_version_id,
            dataset_id=self.dataset_id,
            max_wait=self.max_wait_sec,
            network_egress_policy=context["params"].get("network_egress_policy", None),
            maximum_memory=context["params"].get("maximum_memory", None),
            replicas=context["params"].get("replicas", None),
        )

        self.log.info(f"Overall testing status: {custom_model_test.overall_status}")

        return custom_model_test.id


class GetCustomModelTestOverallStatusOperator(BaseDatarobotOperator):
    """
    Get a custom model testing overall status.

    :param custom_model_test_id: The ID of the custom model test.
    :type custom_model_test_id: str
    :return: custom model test overall status
    :rtype: dict
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["custom_model_test_id"]

    def __init__(
        self,
        *,
        custom_model_test_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_model_test_id = custom_model_test_id

    def validate(self) -> None:
        if self.custom_model_test_id is None:
            raise ValueError("custom_model_test_id is required attribute")

    def execute(self, context: Context) -> str:
        custom_model_test = dr.CustomModelTest.get(custom_model_test_id=self.custom_model_test_id)

        self.log.info(f"Overall testing status: {custom_model_test.overall_status}")

        return custom_model_test.overall_status


class CreateCustomModelDeploymentOperator(BaseDatarobotOperator):
    """
    Create a deployment from a DataRobot custom model image.

    :param custom_model_version_id: id of the DataRobot custom model version to deploy
        The version must have a base_environment_id.
    :type custom_model_version_id: str
    :param deployment_name: a human readable label (name) of the deployment
    :type deployment_name: str
    :param default_prediction_server_id: an identifier of a prediction server to be used as the default prediction server
    :type default_prediction_server_id: str, optional
    :param description: a human readable description of the deployment
    :type description: str, optional
    :param importance: deployment importance
    :type importance: str, optional
    :param max_wait_sec: seconds to wait for successful resolution of a deployment creation job.
        Deployment supports making predictions only after a deployment creating job
        has successfully finished
    :type max_wait_sec: int, optional
    :return: The created deployment id
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "custom_model_version_id",
        "deployment_name",
        "prediction_server_id",
    ]

    def __init__(
        self,
        *,
        custom_model_version_id: str,
        deployment_name: str,
        prediction_server_id: Optional[str] = None,
        description: Optional[str] = None,
        importance: Optional[str] = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_model_version_id = custom_model_version_id
        self.deployment_name = deployment_name
        self.prediction_server_id = prediction_server_id
        self.description = description
        self.importance = importance
        self.max_wait_sec = max_wait_sec

    def validate(self) -> None:
        if self.custom_model_version_id is None:
            raise ValueError("Invalid or missing `custom_model_version_id` value")

        if self.deployment_name is None:
            raise ValueError("Invalid or missing `deployment_name` value")

    def execute(self, context: Context) -> str:
        deployment = dr.Deployment.create_from_custom_model_version(
            custom_model_version_id=self.custom_model_version_id,
            label=self.deployment_name,
            description=self.description,
            default_prediction_server_id=self.prediction_server_id,
            importance=self.importance,
            max_wait=self.max_wait_sec,
        )

        self.log.info(f"Deployment created, deployment_id: {deployment.id}")
        return deployment.id
