# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest
from datarobot.errors import AsyncProcessUnsuccessfulError

from datarobot_provider.sensors.model_insights import ComputeFeatureImpactSensor


def test_compute_feature_impact_sensor__success(mocker):
    job_mock = mocker.Mock()
    job_mock.status = "COMPLETED"
    mocker.patch.object(dr.FeatureImpactJob, "get", return_value=job_mock)

    operator = ComputeFeatureImpactSensor(
        task_id="check_compute_feature_impact",
        project_id="project-id",
        job_id="job-id",
    )
    result = operator.poke(context={})

    assert result is True


def test_compute_feature_impact_sensor__not_finished(mocker):
    job_mock = mocker.Mock()
    job_mock.status = "queue"
    mocker.patch.object(dr.FeatureImpactJob, "get", return_value=job_mock)

    operator = ComputeFeatureImpactSensor(
        task_id="check_compute_feature_impact",
        project_id="project-id",
        job_id="job-id",
    )
    result = operator.poke(context={})

    assert result is False


def test_compute_feature_impact_sensor__raise_error(mocker):
    job_mock = mocker.Mock()
    job_mock.status = "ABORT"
    mocker.patch.object(dr.FeatureImpactJob, "get", return_value=job_mock)

    operator = ComputeFeatureImpactSensor(
        task_id="check_compute_feature_impact",
        project_id="project-id",
        job_id="job-id",
    )
    with pytest.raises(AsyncProcessUnsuccessfulError):
        operator.poke(context={})
