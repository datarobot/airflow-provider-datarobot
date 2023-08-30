# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from datetime import datetime

from airflow.decorators import dag
from datarobot import TARGET_TYPE

from datarobot_provider.operators.custom_models import (
    CreateExecutionEnvironmentOperator,
    CreateCustomInferenceModelOperator,
    CreateCustomModelVersionOperator,
)
from datarobot_provider.operators.custom_models import (
    CreateExecutionEnvironmentVersionOperator,
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['example', 'custom model'],
    params={
        "execution_environment_name": "Demo Execution Environment",
        "execution_environment_description": "Demo Execution Environment for Airflow provider",
        "programming_language": "python",
        "required_metadata_keys": [{"field_name": "test_key", "display_name": "test_display_name"}],
        "docker_context_path": "./datarobot-user-models/public_dropin_environments/python3_pytorch/",
        "custom_model_folder": "./datarobot-user-models/model_templates/python3_pytorch/",
        "custom_model_description": 'This is a custom model created by Airflow',
        "environment_version_description": "created by Airflow provider",
        "custom_model_name": "Airflow Custom model Demo",
        "target_type": TARGET_TYPE.REGRESSION,
        "target_name": 'Grade 2014',
    },
)
def create_custom_model_pipeline():
    create_execution_environment_op = CreateExecutionEnvironmentOperator(
        task_id='create_execution_environment',
    )

    create_execution_environment_version_op = CreateExecutionEnvironmentVersionOperator(
        task_id='create_execution_environment_version',
        execution_environment_id=create_execution_environment_op.output,
    )

    create_custom_inference_model_op = CreateCustomInferenceModelOperator(
        task_id='create_custom_inference_model',
    )

    create_custom_model_version_op = CreateCustomModelVersionOperator(
        task_id='create_custom_model_version',
        custom_model_id=create_custom_inference_model_op.output,
        base_environment_id=create_execution_environment_op.output,
    )

    (
        create_execution_environment_op
        >> create_execution_environment_version_op
        >> create_custom_inference_model_op
        >> create_custom_model_version_op
    )


create_custom_model_pipeline_dag = create_custom_model_pipeline()

if __name__ == "__main__":
    create_custom_model_pipeline_dag.test()
