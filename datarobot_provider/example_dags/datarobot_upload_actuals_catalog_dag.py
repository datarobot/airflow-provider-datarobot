# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag:
{
    "actual_value_column": 'ACTUAL',
    "association_id_column": 'id',
    "timestamp_column": 'timestamp',
    "was_acted_on_column": 'acted',
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.mlops import SubmitActualsFromCatalogOperator
from datarobot_provider.sensors.client import BaseAsyncResolutionSensor


@dag(
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    # Default json config example:
    params={
        "association_id_column": "id",  # column name with a unique identifier used with a prediction.
        "actual_value_column": "ACTUAL",  # column name with the actual value of a prediction.
        "timestamp_column": "",  # optional - column name with datetime or string in RFC3339 format.
        "was_acted_on_column": "",  # optional
    },
)
def datarobot_submit_actuals_from_ai_catalog(deployment_id=None, dataset_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")
    if not dataset_id:
        raise ValueError("Invalid or missing `dataset_id` value")

    upload_actuals_op = SubmitActualsFromCatalogOperator(
        task_id="upload_actuals",
        deployment_id=deployment_id,
        # dataset_id can be received from previous operator
        dataset_id=dataset_id,
    )

    uploading_complete_sensor = BaseAsyncResolutionSensor(
        task_id="check_uploading_actuals_complete",
        job_id=upload_actuals_op.output,
        poke_interval=5,
        mode="reschedule",
        timeout=3600,
    )

    upload_actuals_op >> uploading_complete_sensor


datarobot_upload_actuals_from_ai_catalog_dag = datarobot_submit_actuals_from_ai_catalog()

if __name__ == "__main__":
    datarobot_upload_actuals_from_ai_catalog_dag.test()
