# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections import namedtuple
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import datarobot as dr
import pytest
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowFailException
from datarobot import Model
from datarobot.insights import ShapImpact
from datarobot.insights import ShapPreview

from datarobot_provider.operators.model_insights import ComputeFeatureEffectsOperator
from datarobot_provider.operators.model_insights import ComputeFeatureImpactOperator
from datarobot_provider.operators.model_insights import ComputeShapImpactOperator
from datarobot_provider.operators.model_insights import ComputeShapPreviewOperator
from datarobot_provider.operators.model_insights import GetFeaturesUsedOperator
from datarobot_provider.operators.model_insights import GetLiftChartInsightOperator
from datarobot_provider.operators.model_insights import GetResidualsChartInsightOperator
from datarobot_provider.operators.model_insights import GetRocCurveInsightOperator


@pytest.mark.parametrize(
    "project_id, model_id, expected_exception, match",
    [
        ("project-id", "model-id", None, None),
        (None, "model-id", ValueError, "project_id is required."),
        ("project-id", None, ValueError, "model_id is required."),
    ],
)
def test_get_features_used_operator_validate(project_id, model_id, expected_exception, match):
    operator = GetFeaturesUsedOperator(
        task_id="get_features_used", project_id=project_id, model_id=model_id
    )
    if expected_exception:
        with pytest.raises(expected_exception, match=match):
            operator.validate()
    else:
        operator.validate()


@patch("datarobot_provider.operators.model_insights.dr.models.Model.get")
def test_get_features_used_operator_execute(mock_get_model):
    # Setup the mock
    mock_model = MagicMock()
    mock_model.get_features_used.return_value = ["feature1", "feature2", "feature3"]
    mock_get_model.return_value = mock_model

    # Create and execute the operator
    operator = GetFeaturesUsedOperator(
        task_id="get_features_used", project_id="project-id", model_id="model-id"
    )
    result = operator.execute(context={})

    # Verify the results
    assert result == ["feature1", "feature2", "feature3"]
    mock_get_model.assert_called_once_with("project-id", "model-id")
    mock_model.get_features_used.assert_called_once()


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
    model_id = "test-model-id"
    job_id = 123

    job_mock = mocker.Mock()
    job_mock.job_id = job_id

    request_shap_mock = mocker.patch.object(ShapPreview, "compute", return_value=job_mock)

    operator = ComputeShapPreviewOperator(task_id="compute_shap", model_id=model_id)

    result = operator.execute(context={"params": {}})

    request_shap_mock.assert_called_with(entity_id=model_id)
    assert result == 123


def test_operator_compute_shap_no_model_id():
    with pytest.raises(AirflowException):
        ComputeShapPreviewOperator(task_id="compute_shap")


def test_operator_compute_shap_impact(mocker):
    model_id = "test-model-id"
    job_id = 123

    job_mock = mocker.Mock()
    job_mock.job_id = job_id

    request_shap_mock = mocker.patch.object(ShapImpact, "compute", return_value=job_mock)

    operator = ComputeShapImpactOperator(task_id="compute_shap", model_id=model_id)

    result = operator.execute(context={"params": {}})

    request_shap_mock.assert_called_with(entity_id=model_id)
    assert result == 123


def test_operator_compute_shap_impact_no_model_id():
    with pytest.raises(AirflowException):
        ComputeShapImpactOperator(task_id="compute_shap")


def test_operator_get_roc_curve_insight(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    RocResults = namedtuple(
        "RocResults", ["roc_points", "positive_class_predictions", "negative_class_predictions"]
    )
    roc_data = RocResults(
        roc_points=[{"fpr": 0.1, "tpr": 0.9}],
        positive_class_predictions=[0.9],
        negative_class_predictions=[0.1],
    )

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id
    model_mock.get_roc_curve.return_value = roc_data

    get_model_mock = mocker.patch.object(Model, "get", return_value=model_mock)

    operator = GetRocCurveInsightOperator(
        task_id="get_roc_curve_insight", project_id=project_id, model_id=model_id
    )

    result = operator.execute(context={"params": {}})

    get_model_mock.assert_called_with(project_id, model_id)
    model_mock.get_roc_curve.assert_called_with(source="validation")
    assert result == {
        "roc_points": roc_data.roc_points,
        "positive_class_predictions": roc_data.positive_class_predictions,
        "negative_class_predictions": roc_data.negative_class_predictions,
    }


def test_operator_get_roc_curve_insight_no_project_id(mocker):
    model_id = "test-model-id"

    operator = GetRocCurveInsightOperator(
        task_id="get_roc_curve_insight", project_id="", model_id=model_id
    )

    with pytest.raises(AirflowFailException):
        operator.validate()


def test_operator_get_roc_curve_insight_no_model_id(mocker):
    project_id = "test-project-id"

    operator = GetRocCurveInsightOperator(
        task_id="get_roc_curve_insight", project_id=project_id, model_id=""
    )

    with pytest.raises(AirflowFailException):
        operator.validate()


def test_operator_get_lift_chart_insight(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    lift_chart = Mock(
        bins=[
            {
                "actual": 0.1,
                "predicted": 0.9,
                "bin_weight": 10.0,
            },
            {
                "actual": 0.2,
                "predicted": 0.10,
                "bin_weight": 11.0,
            },
        ]
    )

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id
    model_mock.get_lift_chart.return_value = lift_chart

    get_model_mock = mocker.patch.object(Model, "get", return_value=model_mock)

    operator = GetLiftChartInsightOperator(
        task_id="get_lift_chart_insight", project_id=project_id, model_id=model_id
    )

    result = operator.execute(context={"params": {}})

    get_model_mock.assert_called_with(project_id, model_id)
    model_mock.get_lift_chart.assert_called_with(source="validation")
    assert result == lift_chart.bins


def test_operator_get_lift_chart_insight_no_project_id(mocker):
    model_id = "test-model-id"

    operator = GetLiftChartInsightOperator(
        task_id="get_lift_chart_insight", project_id="", model_id=model_id
    )

    with pytest.raises(AirflowFailException):
        operator.validate()


def test_operator_get_lift_chart_insight_no_model_id(mocker):
    project_id = "test-project-id"

    operator = GetLiftChartInsightOperator(
        task_id="get_lift_chart_insight", project_id=project_id, model_id=""
    )

    with pytest.raises(AirflowFailException):
        operator.validate()


def test_operator_get_residuals_chart_insight(mocker):
    project_id = "test-project-id"
    model_id = "test-model-id"
    ResidualsResults = namedtuple(
        "ResidualsResults", ["residual_mean", "coefficient_of_determination", "standard_deviation"]
    )
    residuals_data = ResidualsResults(
        residual_mean=0.1,
        coefficient_of_determination=0.2,
        standard_deviation=0.3,
    )

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id
    model_mock.get_residuals_chart.return_value = residuals_data

    get_model_mock = mocker.patch.object(Model, "get", return_value=model_mock)

    operator = GetResidualsChartInsightOperator(
        task_id="get_residuals_chart_insight", project_id=project_id, model_id=model_id
    )

    result = operator.execute(context={"params": {}})

    get_model_mock.assert_called_with(project_id, model_id)
    model_mock.get_residuals_chart.assert_called_with(source="validation")
    assert result == {
        "residual_mean": residuals_data.residual_mean,
        "coefficient_of_determination": residuals_data.coefficient_of_determination,
        "standard_deviation": residuals_data.standard_deviation,
    }


def test_operator_get_residuals_chart_insight_no_project_id(mocker):
    model_id = "test-model-id"

    operator = GetResidualsChartInsightOperator(
        task_id="get_residuals_chart_insight", project_id="", model_id=model_id
    )

    with pytest.raises(AirflowFailException):
        operator.validate()


def test_operator_get_residuals_chart_insight_no_model_id(mocker):
    project_id = "test-project-id"

    operator = GetResidualsChartInsightOperator(
        task_id="get_residuals_chart_insight", project_id=project_id, model_id=""
    )

    with pytest.raises(AirflowFailException):
        operator.validate()
