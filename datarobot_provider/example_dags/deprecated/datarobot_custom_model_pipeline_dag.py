# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datarobot import TARGET_TYPE
from datarobot.enums import DEPLOYMENT_IMPORTANCE

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator
from datarobot_provider.operators.custom_models import CreateCustomInferenceModelOperator
from datarobot_provider.operators.custom_models import CreateCustomModelDeploymentOperator
from datarobot_provider.operators.custom_models import CreateCustomModelVersionOperator
from datarobot_provider.operators.custom_models import CreateExecutionEnvironmentOperator
from datarobot_provider.operators.custom_models import CreateExecutionEnvironmentVersionOperator
from datarobot_provider.operators.custom_models import CustomModelTestOperator
from datarobot_provider.operators.custom_models import GetCustomModelTestOverallStatusOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "custom model"],
    params={
        "execution_environment_name": "Execution Env Airflow",
        "execution_environment_description": "Demo Execution Environment created by Airflow provider",
        "programming_language": "python",
        "required_metadata_keys": [],  # example: [{"field_name": "test_key", "display_name": "test_display_name"}],
        "custom_model_description": "This is a custom model created by Airflow",
        "environment_version_description": "created by Airflow provider",
        "custom_model_name": "Airflow Custom model Demo",
        "target_type": TARGET_TYPE.REGRESSION,
        "target_name": "Grade 2014",
        "is_major_update": True,
        "is_training_data_for_versions_permanently_enabled": True,
        "docker_context_path": "/usr/local/airflow/dags/datarobot-user-models/public_dropin_environments/python3_pytorch/",
        "custom_model_folder": "/usr/local/airflow/dags/datarobot-user-models/model_templates/python3_pytorch/",
        "test_dataset_file_path": "/usr/local/airflow/dags/datarobot-user-models/tests/testdata/juniors_3_year_stats_regression.csv",
        "train_dataset_file_path": "/usr/local/airflow/dags/datarobot-user-models/tests/testdata/juniors_3_year_stats_regression.csv",
    },
)
def create_custom_model_pipeline(
    prediction_server_id="5fbc1924ccfc5a0025c424bf", deployment_name="Demo Deployment Airflow"
):
    create_execution_environment_op = CreateExecutionEnvironmentOperator(
        task_id="create_execution_environment",
    )

    create_execution_environment_version_op = CreateExecutionEnvironmentVersionOperator(
        task_id="create_execution_environment_version",
        execution_environment_id=create_execution_environment_op.output,
    )

    create_custom_inference_model_op = CreateCustomInferenceModelOperator(
        task_id="create_custom_inference_model",
    )

    train_dataset_uploading_op = UploadDatasetOperator(
        task_id="train_dataset_uploading", file_path_param="train_dataset_file_path"
    )

    create_custom_model_version_op = CreateCustomModelVersionOperator(
        task_id="create_custom_model_version",
        custom_model_id=create_custom_inference_model_op.output,
        base_environment_id=create_execution_environment_op.output,
        training_dataset_id=train_dataset_uploading_op.output,
    )

    test_dataset_uploading_op = UploadDatasetOperator(
        task_id="test_dataset_uploading", file_path_param="test_dataset_file_path"
    )

    custom_model_test_op = CustomModelTestOperator(
        task_id="custom_model_test",
        custom_model_id=create_custom_inference_model_op.output,
        custom_model_version_id=create_custom_model_version_op.output,
        dataset_id=test_dataset_uploading_op.output,
    )

    custom_model_test_overall_status_op = GetCustomModelTestOverallStatusOperator(
        task_id="custom_model_test_overall_status",
        custom_model_test_id=custom_model_test_op.output,
    )

    def choose_branch(custom_model_test_overall_status):
        if custom_model_test_overall_status == "succeeded":
            return ["deploy_custom_model"]
        return ["custom_model_test_fail"]

    branching = BranchPythonOperator(
        task_id="if_tests_succeed",
        python_callable=choose_branch,
        op_args=[custom_model_test_overall_status_op.output],
    )

    deploy_custom_model_op = CreateCustomModelDeploymentOperator(
        task_id="deploy_custom_model",
        custom_model_version_id=create_custom_model_version_op.output,
        deployment_name=deployment_name,
        prediction_server_id=prediction_server_id,
        importance=DEPLOYMENT_IMPORTANCE.LOW,
    )

    custom_model_test_fail_case_op = EmptyOperator(
        task_id="custom_model_test_fail",
    )

    (
        create_execution_environment_op
        >> create_execution_environment_version_op
        >> create_custom_inference_model_op
        >> train_dataset_uploading_op
        >> create_custom_model_version_op
        >> test_dataset_uploading_op
        >> custom_model_test_op
        >> custom_model_test_overall_status_op
        >> branching
        >> [deploy_custom_model_op, custom_model_test_fail_case_op]
    )


create_custom_model_pipeline_dag = create_custom_model_pipeline()

if __name__ == "__main__":
    create_custom_model_pipeline_dag.test()
