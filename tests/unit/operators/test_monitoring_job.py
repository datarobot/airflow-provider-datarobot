# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

import datarobot as dr
import pytest
from datarobot_provider.operators.monitoring_job import BatchMonitoringOperator

@pytest.fixture(
    scope="module",
    params=[
        {
            "type": "dataset",
            "dataset_id": "dataset-id",
        },
        {
            "type": "s3",
            "url": "s3://path/to/scoring_dataset.csv",
            "credential_id": "credential-id",
        },
    ],
)
def monitoring_settings():
    return {
        "intake_settings":  {
            "type": "dataset",
            "dataset_id": "dataset-id",
        },
        "monitoring_columns": {
            "predictions_columns": [
                {
                   "class_name": "True",
                   "column_name": "target_True_PREDICTION"
                },
                {
                   "class_name": "False",
                   "column_name": "target_False_PREDICTION"
                }
            ],
            "association_id_column": "id",
            "actuals_value_column": "ACTUAL",
        },
    }

def test_operator_batch_monitoring_job(mocker, monitoring_settings):
    job_id = "job-id"
    deployment_id = "deployment-id"

    job_mock = mocker.Mock()
    job_mock.id = job_id
    monitoring_job_run_mock = mocker.patch.object(dr.BatchMonitoringJob, "run", return_value=job_mock)

    expected_intake_settings = monitoring_settings["intake_settings"].copy()

    if monitoring_settings["intake_settings"]["type"] == "dataset":
        dataset_mock = mocker.Mock()
        dataset_mock.id = "dataset-id"
        mocker.patch.object(dr.Dataset, "get", return_value=dataset_mock)

        del expected_intake_settings["dataset_id"]
        expected_intake_settings["dataset"] = dataset_mock

    operator = BatchMonitoringOperator(task_id="batch_monitoring", deployment_id=deployment_id)

    result = operator.execute(
        context={
            "params": {
                "monitoring_settings": monitoring_settings,
            }
        }
    )

    monitoring_job_run_mock.assert_called_with(
        deployment_id,
        intake_settings=expected_intake_settings,
        monitoring_columns=monitoring_settings["monitoring_columns"],
    )
    assert result == job_id


def test_operator_batch_monitoring_job_fails_when_no_dataset_id():
    operator = BatchMonitoringOperator(task_id="batch_monitoring", deployment_id="deployment-id")

    # should raise ValueError if intake type is `dataset` but no dataset_id is supplied
    with pytest.raises(ValueError):
        operator.execute(
            context={
                "params": {
                    "monitoring_settings": {
                        "intake_settings": {
                            "type": "dataset",
                        },
                        "monitoring_columns": {
                            "predictions_columns": [
                                {
                                    "class_name": "True",
                                    "column_name": "target_True_PREDICTION"
                                },
                                {
                                    "class_name": "False",
                                    "column_name": "target_False_PREDICTION"
                                }
                            ],
                            "association_id_column": "id",
                            "actuals_value_column": "ACTUAL",
                        },
                    },
                }
            }
        )
