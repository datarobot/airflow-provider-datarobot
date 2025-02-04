# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest
from datarobot import NETWORK_EGRESS_POLICY
from datarobot import TARGET_TYPE

from datarobot_provider.operators.custom_models import CreateCustomInferenceModelOperator
from datarobot_provider.operators.custom_models import CreateCustomModelDeploymentOperator
from datarobot_provider.operators.custom_models import CreateCustomModelVersionOperator
from datarobot_provider.operators.custom_models import CustomModelTestOperator
from datarobot_provider.operators.custom_models import GetCustomModelTestOverallStatusOperator


@pytest.fixture
def custom_model_params():
    return {
        "execution_environment_name": "Demo Execution Environment",
        "execution_environment_description": "Demo Execution Environment for Airflow provider",
        "programming_language": "python",
        "required_metadata_keys": [{"field_name": "test_key", "display_name": "test_display_name"}],
        "docker_context_path": "./datarobot-user-models/public_dropin_environments/python3_pytorch/",
        "custom_model_folder": "./datarobot-user-models/model_templates/python3_pytorch/",
        "custom_model_description": "This is a custom model created by Airflow",
        "environment_version_description": "created by Airflow provider",
        "custom_model_name": "Airflow Custom model Demo",
        "target_type": TARGET_TYPE.REGRESSION,
        "target_name": "Grade 2014",
        "is_major_update": True,
        "files": ["file1", "file2"],
        "files_to_delete": ["file3", "file4"],
        "network_egress_policy": NETWORK_EGRESS_POLICY.ALL,
        "maximum_memory": 2048,
        "replicas": 1,
        "required_metadata_values": [],
        "partition_column": "test",
        "keep_training_holdout_data": False,
        "negative_class_label": "0",
        "positive_class_label": "1",
        "prediction_threshold": 0.5,
        "class_labels": ["0", "1"],
        "class_labels_file": "file",
        "is_training_data_for_versions_permanently_enabled": False,
    }


def test_operator_create_custom_model_op(mocker, custom_model_params):
    custom_model_mock = mocker.Mock(target=None)
    custom_model_mock.id = "test-custom-model-id"

    custom_model_create_mock = mocker.patch.object(
        dr.CustomInferenceModel, "create", return_value=custom_model_mock
    )

    operator = CreateCustomInferenceModelOperator(
        task_id="create_custom_inference_model",
    )

    operator_result = operator.execute(context={"params": custom_model_params})

    custom_model_create_mock.assert_called_with(
        name=custom_model_params["custom_model_name"],
        description=custom_model_params["custom_model_description"],
        target_type=custom_model_params["target_type"],
        target_name=custom_model_params["target_name"],
        language=custom_model_params["programming_language"],
        negative_class_label=custom_model_params["negative_class_label"],
        network_egress_policy=custom_model_params["network_egress_policy"],
        positive_class_label=custom_model_params["positive_class_label"],
        prediction_threshold=custom_model_params["prediction_threshold"],
        replicas=custom_model_params["replicas"],
        class_labels=custom_model_params["class_labels"],
        class_labels_file=custom_model_params["class_labels_file"],
        maximum_memory=custom_model_params["maximum_memory"],
        is_training_data_for_versions_permanently_enabled=custom_model_params[
            "is_training_data_for_versions_permanently_enabled"
        ],
    )

    assert operator_result == custom_model_mock.id


def test_operator_create_custom_model_name_not_provided_op(mocker, custom_model_params):
    custom_model_mock = mocker.Mock(target=None)
    custom_model_mock.id = "test-custom-model-id"

    operator = CreateCustomInferenceModelOperator(
        task_id="create_custom_inference_model",
    )

    custom_model_params_name_not_provided = custom_model_params.copy()
    custom_model_params_name_not_provided.pop("custom_model_name", None)

    with pytest.raises(ValueError):
        operator.execute(context={"params": custom_model_params_name_not_provided})


def test_operator_create_custom_model_target_type_not_provided_op(mocker, custom_model_params):
    custom_model_mock = mocker.Mock(target=None)
    custom_model_mock.id = "test-custom-model-id"

    operator = CreateCustomInferenceModelOperator(
        task_id="create_custom_inference_model",
    )

    custom_model_params_target_type_not_provided = custom_model_params.copy()
    custom_model_params_target_type_not_provided.pop("target_type", None)

    with pytest.raises(ValueError):
        operator.execute(context={"params": custom_model_params_target_type_not_provided})


def test_operator_create_custom_model_version_op(mocker, custom_model_params):
    custom_model_version_mock = mocker.Mock(target=None)
    custom_model_version_mock.id = "test-custom-model-version-id"

    custom_model_version_create_clean_mock = mocker.patch.object(
        dr.CustomModelVersion, "create_clean", return_value=custom_model_version_mock
    )

    custom_model_version_create_from_previous_mock = mocker.patch.object(
        dr.CustomModelVersion, "create_from_previous", return_value=custom_model_version_mock
    )

    custom_model_id = "custom-model-id"
    training_dataset_id = "training-dataset-id"
    base_environment_id = "base-environment-id"
    holdout_dataset_id = "holdout-dataset-id"

    operator = CreateCustomModelVersionOperator(
        task_id="create_custom_model_version",
        custom_model_id=custom_model_id,
        training_dataset_id=training_dataset_id,
        base_environment_id=base_environment_id,
        holdout_dataset_id=holdout_dataset_id,
    )

    operator_result = operator.execute(context={"params": custom_model_params})

    custom_model_version_create_clean_mock.assert_called_with(
        custom_model_id=custom_model_id,
        training_dataset_id=training_dataset_id,
        base_environment_id=base_environment_id,
        holdout_dataset_id=holdout_dataset_id,
        folder_path=custom_model_params["custom_model_folder"],
        is_major_update=custom_model_params["is_major_update"],
        files=custom_model_params["files"],
        network_egress_policy=custom_model_params["network_egress_policy"],
        maximum_memory=custom_model_params["maximum_memory"],
        replicas=custom_model_params["replicas"],
        required_metadata_values=custom_model_params["required_metadata_values"],
        partition_column=custom_model_params["partition_column"],
        keep_training_holdout_data=custom_model_params["keep_training_holdout_data"],
        max_wait=3600,
    )

    custom_model_version_create_from_previous_mock.assert_not_called()

    assert operator_result == custom_model_version_mock.id


def test_operator_create_custom_model_version_from_previous_op(mocker, custom_model_params):
    custom_model_version_mock = mocker.Mock(target=None)
    custom_model_version_mock.id = "test-custom-model-version-id"

    custom_model_version_create_clean_mock = mocker.patch.object(
        dr.CustomModelVersion, "create_clean", return_value=custom_model_version_mock
    )

    custom_model_version_create_from_previous_mock = mocker.patch.object(
        dr.CustomModelVersion, "create_from_previous", return_value=custom_model_version_mock
    )

    custom_model_id = "custom-model-id"
    training_dataset_id = "training-dataset-id"
    base_environment_id = "base-environment-id"
    holdout_dataset_id = "holdout-dataset-id"

    operator = CreateCustomModelVersionOperator(
        task_id="create_custom_model_version",
        custom_model_id=custom_model_id,
        training_dataset_id=training_dataset_id,
        base_environment_id=base_environment_id,
        holdout_dataset_id=holdout_dataset_id,
        create_from_previous=True,
    )

    operator_result = operator.execute(context={"params": custom_model_params})

    custom_model_version_create_from_previous_mock.assert_called_with(
        custom_model_id=custom_model_id,
        training_dataset_id=training_dataset_id,
        base_environment_id=base_environment_id,
        holdout_dataset_id=holdout_dataset_id,
        folder_path=custom_model_params["custom_model_folder"],
        is_major_update=custom_model_params["is_major_update"],
        files=custom_model_params["files"],
        files_to_delete=custom_model_params["files_to_delete"],
        network_egress_policy=custom_model_params["network_egress_policy"],
        maximum_memory=custom_model_params["maximum_memory"],
        replicas=custom_model_params["replicas"],
        required_metadata_values=custom_model_params["required_metadata_values"],
        partition_column=custom_model_params["partition_column"],
        keep_training_holdout_data=custom_model_params["keep_training_holdout_data"],
        max_wait=3600,
    )

    custom_model_version_create_clean_mock.assert_not_called()

    assert operator_result == custom_model_version_mock.id


def test_operator_create_custom_model_version_no_custom_model_id_op(mocker, custom_model_params):
    custom_model_version_mock = mocker.Mock(target=None)
    custom_model_version_mock.id = "test-custom-model-version-id"

    training_dataset_id = "training-dataset-id"
    base_environment_id = "base-environment-id"
    holdout_dataset_id = "holdout-dataset-id"

    operator = CreateCustomModelVersionOperator(
        task_id="create_custom_model_version",
        custom_model_id=None,
        training_dataset_id=training_dataset_id,
        base_environment_id=base_environment_id,
        holdout_dataset_id=holdout_dataset_id,
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_create_custom_model_test_op(mocker, custom_model_params):
    custom_model_test_mock = mocker.Mock(target=None)
    custom_model_test_mock.id = "test-custom-model-test-id"
    custom_model_test_mock.overall_status = "test"

    custom_model_test_create_mock = mocker.patch.object(
        dr.CustomModelTest, "create", return_value=custom_model_test_mock
    )

    custom_model_id = "custom-model-id"
    dataset_id = "dataset-id"
    custom_model_version_id = "custom-model-version-id"
    max_wait_sec = 1000

    operator = CustomModelTestOperator(
        task_id="create_custom_model_test",
        custom_model_id=custom_model_id,
        custom_model_version_id=custom_model_version_id,
        dataset_id=dataset_id,
        max_wait_sec=max_wait_sec,
    )

    operator_result = operator.execute(context={"params": custom_model_params})

    custom_model_test_create_mock.assert_called_with(
        custom_model_id=custom_model_id,
        custom_model_version_id=custom_model_version_id,
        dataset_id=dataset_id,
        network_egress_policy=custom_model_params["network_egress_policy"],
        maximum_memory=custom_model_params["maximum_memory"],
        replicas=custom_model_params["replicas"],
        max_wait=max_wait_sec,
    )

    assert operator_result == custom_model_test_mock.id


def test_operator_create_custom_model_test_no_custom_model_id_op():
    custom_model_id = None
    dataset_id = "dataset-id"
    custom_model_version_id = "custom-model-version-id"
    max_wait_sec = 1000

    operator = CustomModelTestOperator(
        task_id="create_custom_model_test",
        custom_model_id=custom_model_id,
        custom_model_version_id=custom_model_version_id,
        dataset_id=dataset_id,
        max_wait_sec=max_wait_sec,
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_create_custom_model_test_no_custom_model_version_id_op():
    custom_model_id = "custom-model-id"
    dataset_id = "dataset-id"
    custom_model_version_id = None
    max_wait_sec = 1000

    operator = CustomModelTestOperator(
        task_id="create_custom_model_test",
        custom_model_id=custom_model_id,
        custom_model_version_id=custom_model_version_id,
        dataset_id=dataset_id,
        max_wait_sec=max_wait_sec,
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_create_custom_model_test_status_op(mocker, custom_model_params):
    custom_model_test_status_mock = mocker.Mock(target=None)
    custom_model_test_status_mock.id = "test-custom-model-test-id"
    custom_model_test_status_mock.overall_status = "completed"

    custom_model_test_get_mock = mocker.patch.object(
        dr.CustomModelTest, "get", return_value=custom_model_test_status_mock
    )

    custom_model_test_id = "custom-model-test-id"

    operator = GetCustomModelTestOverallStatusOperator(
        task_id="get_custom_model_test_overall_status",
        custom_model_test_id=custom_model_test_id,
    )

    operator_result = operator.execute(context={"params": custom_model_params})

    custom_model_test_get_mock.assert_called_with(
        custom_model_test_id=custom_model_test_id,
    )

    assert operator_result == custom_model_test_status_mock.overall_status


def test_operator_get_custom_model_test_no_custom_model_test_id_op():
    custom_model_test_id = "custom-model-test-id"

    operator = GetCustomModelTestOverallStatusOperator(
        task_id="create_custom_model_test",
        custom_model_test_id=custom_model_test_id,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": custom_model_params})


def test_operator_create_custom_model_deployment_op(mocker, custom_model_params):
    custom_model_deployment_mock = mocker.Mock(target=None)
    custom_model_deployment_mock.id = "custom-model-deployment-id"

    create_custom_model_deployment_mock = mocker.patch.object(
        dr.Deployment, "create_from_custom_model_version", return_value=custom_model_deployment_mock
    )

    custom_model_version_id = "custom-model-version-id"
    prediction_server_id = "prediction-server-id"
    deployment_name = "Demo Test"
    description = "Test Description"
    max_wait_sec = 1000
    importance = dr.enums.DEPLOYMENT_IMPORTANCE.LOW

    operator = CreateCustomModelDeploymentOperator(
        task_id="deploy_custom_model",
        custom_model_version_id=custom_model_version_id,
        deployment_name=deployment_name,
        description=description,
        prediction_server_id=prediction_server_id,
        importance=importance,
        max_wait_sec=max_wait_sec,
    )

    operator_result = operator.execute(context={"params": custom_model_params})

    create_custom_model_deployment_mock.assert_called_with(
        custom_model_version_id=custom_model_version_id,
        label=deployment_name,
        description=description,
        default_prediction_server_id=prediction_server_id,
        importance=importance,
        max_wait=max_wait_sec,
    )

    assert operator_result == custom_model_deployment_mock.id


def test_operator_create_custom_model_deployment_no_custom_model_id_op():
    custom_model_version_id = None
    deployment_name = "deployment-name"

    operator = CreateCustomModelDeploymentOperator(
        task_id="deploy_custom_model",
        custom_model_version_id=custom_model_version_id,
        deployment_name=deployment_name,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": custom_model_params})


def test_operator_create_custom_model_deployment_no_deployment_name_op():
    custom_model_version_id = "custom-model-test-id"
    deployment_name = None

    operator = CreateCustomModelDeploymentOperator(
        task_id="deploy_custom_model",
        custom_model_version_id=custom_model_version_id,
        deployment_name=deployment_name,
    )

    with pytest.raises(ValueError):
        operator.execute(context={"params": custom_model_params})
