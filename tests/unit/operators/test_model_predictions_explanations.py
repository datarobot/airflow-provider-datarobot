# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.operators.prediction_explanations import (
    ComputePredictionExplanationsOperator,
    PredictionExplanationsInitializationOperator,
)


def test_operator_model_prediction_explanations_initialization(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    job_id = "test-predictions-explanations-initialization-job-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    pe_exp_init_job_mock = mocker.Mock()
    pe_exp_init_job_mock.id = job_id

    request_pe_exp_init_mock = mocker.patch.object(
        dr.PredictionExplanationsInitialization, "create", return_value=pe_exp_init_job_mock
    )

    operator = PredictionExplanationsInitializationOperator(
        task_id="prediction_explanations_initialization",
        project_id=project_id,
        model_id=model_id,
    )

    result = operator.execute(context={"params": {}})

    request_pe_exp_init_mock.assert_called_with(project_id, model_id)
    assert result == job_id


def test_operator_compute_model_prediction_explanations(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    external_dataset_id = "test-external-dataset-id"
    job_id = "test-compute-model-predictions-explanations-job-id"

    params = {'threshold_high': 0.9, 'threshold_low': 0.1, 'max_explanations': 3}

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    compute_pe_exp_job_mock = mocker.Mock()
    compute_pe_exp_job_mock.id = job_id

    request_pe_exp_init_mock = mocker.patch.object(
        dr.PredictionExplanations, "create", return_value=compute_pe_exp_job_mock
    )

    operator = ComputePredictionExplanationsOperator(
        task_id="compute_prediction_explanations",
        project_id=project_id,
        model_id=model_id,
        external_dataset_id=external_dataset_id,
    )

    result = operator.execute(context={"params": params})

    request_pe_exp_init_mock.assert_called_with(
        project_id,
        model_id,
        external_dataset_id,
        max_explanations=params["max_explanations"],
        threshold_low=params["threshold_low"],
        threshold_high=params["threshold_high"],
    )
    assert result == job_id
