# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest

from datarobot_provider.operators.model_insights import ComputeFeatureEffectsOperator
from datarobot_provider.operators.model_insights import ComputeFeatureImpactOperator
from datarobot_provider.operators.model_insights import ComputeShapOperator


def test_operator_compute_feature_impact(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    job_id = 123

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id
    model_mock.request_feature_impact()

    get_model_mock = mocker.patch.object(dr.models.Model, "get", return_value=model_mock)
    job_mock = mocker.Mock()
    job_mock.id = job_id

    request_feature_impact_mock = mocker.patch.object(
        model_mock, "request_feature_impact", return_value=job_mock
    )

    operator = ComputeFeatureImpactOperator(
        task_id="compute_feature_impact", project_id=project_id, model_id=model_id
    )

    result = operator.execute(context={"params": {}})

    get_model_mock.assert_called_with(project_id, model_id)
    request_feature_impact_mock.assert_called()
    assert result == job_id


def test_operator_compute_feature_impact_no_project_id(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    operator = ComputeFeatureImpactOperator(
        task_id="compute_feature_impact", model_id=model_id, project_id=""
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_compute_feature_impact_no_model_id(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    operator = ComputeFeatureImpactOperator(
        task_id="compute_feature_impact", model_id="", project_id=project_id
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_compute_feature_effects(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    job_id = 123

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id
    model_mock.request_feature_impact()

    get_model_mock = mocker.patch.object(dr.models.Model, "get", return_value=model_mock)
    job_mock = mocker.Mock()
    job_mock.id = job_id

    request_feature_impact_mock = mocker.patch.object(
        model_mock, "request_feature_effect", return_value=job_mock
    )

    operator = ComputeFeatureEffectsOperator(
        task_id="compute_feature_effect", project_id=project_id, model_id=model_id
    )

    result = operator.execute(context={"params": {}})

    get_model_mock.assert_called_with(project_id, model_id)
    request_feature_impact_mock.assert_called()
    assert result == job_id


def test_operator_compute_feature_effects_no_project_id(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    operator = ComputeFeatureEffectsOperator(
        task_id="compute_feature_effects", project_id="", model_id=model_id
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_compute_feature_effects_no_model_id(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    operator = ComputeFeatureEffectsOperator(
        task_id="compute_feature_effects", project_id=project_id, model_id=""
    )

    with pytest.raises(ValueError):
        operator.validate()


def test_operator_compute_shap(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    job_id = 123

    job_mock = mocker.Mock()
    job_mock.id = job_id

    request_shap_mock = mocker.patch.object(dr.ShapImpact, "create", return_value=job_mock)

    operator = ComputeShapOperator(task_id="compute_shap", project_id=project_id, model_id=model_id)

    result = operator.execute(context={"params": {}})

    request_shap_mock.assert_called_with(project_id=project_id, model_id=model_id)
    assert result == job_id


def test_operator_compute_shap_no_project_id(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    operator = ComputeShapOperator(task_id="compute_shap", model_id=model_id)

    with pytest.raises(ValueError):
        operator.execute(context={"params": {}})


def test_operator_compute_shap_no_model_id(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    operator = ComputeShapOperator(task_id="compute_shap", project_id=project_id)

    with pytest.raises(ValueError):
        operator.execute(context={"params": {}})
