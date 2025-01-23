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
from datarobot.enums import MODEL_REPLACEMENT_REASON

from datarobot_provider.operators.custom_models import CreateCustomModelVersionOperator
from datarobot_provider.operators.custom_models import CustomModelTestOperator
from datarobot_provider.operators.custom_models import GetCustomModelTestOverallStatusOperator
from datarobot_provider.operators.deployment import ReplaceModelOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "custom model"],
    params={
        "programming_language": "python",
        "required_metadata_keys": [],
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
def update_custom_model_pipeline(
    execution_environment_id="650d8b27e53f3f2bb067b58a",
    custom_inference_model_id="650d8db8473c77acc713d239",
    train_dataset_id="65106d23d7fe593f61995bad",
    test_dataset_id="65106d23d7fe593f61995bad",
    deployment_id="650d90d1c03bf7a69afdda85",
):
    create_custom_model_version_op = CreateCustomModelVersionOperator(
        task_id="create_custom_model_version",
        custom_model_id=custom_inference_model_id,
        base_environment_id=execution_environment_id,
        training_dataset_id=train_dataset_id,
    )

    custom_model_test_op = CustomModelTestOperator(
        task_id="custom_model_test",
        custom_model_id=custom_inference_model_id,
        custom_model_version_id=create_custom_model_version_op.output,
        dataset_id=test_dataset_id,
    )

    custom_model_test_overall_status_op = GetCustomModelTestOverallStatusOperator(
        task_id="custom_model_test_overall_status",
        custom_model_test_id=custom_model_test_op.output,
    )

    def choose_branch(custom_model_test_overall_status):
        if custom_model_test_overall_status == "succeeded":
            return ["replace_deployment_model"]
        else:
            return ["model_replacement_fail"]

    branching = BranchPythonOperator(
        task_id="if_tests_succeed",
        python_callable=choose_branch,
        op_args=[
            custom_model_test_overall_status_op.output,
        ],
    )

    handle_model_replacement_fail_op = EmptyOperator(
        task_id="model_replacement_fail",
    )

    replace_deployment_model_op = ReplaceModelOperator(
        task_id="replace_deployment_model",
        deployment_id=deployment_id,
        new_model_id=create_custom_model_version_op.output,
        reason=MODEL_REPLACEMENT_REASON.ACCURACY,
    )

    (
        create_custom_model_version_op
        >> custom_model_test_op
        >> custom_model_test_overall_status_op
        >> branching
        >> [
            replace_deployment_model_op,
            handle_model_replacement_fail_op,
        ]
    )


update_custom_model_pipeline_dag = update_custom_model_pipeline()

if __name__ == "__main__":
    update_custom_model_pipeline_dag.test()
