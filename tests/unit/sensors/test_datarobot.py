# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import datarobot as dr
import pytest
from datarobot.errors import AsyncProcessUnsuccessfulError
from datarobot.models import JobStatusResult
from datarobot.models import StatusCheckJob

from datarobot_provider.sensors.datarobot import AutopilotCompleteSensor
from datarobot_provider.sensors.datarobot import ScoringCompleteSensor
from datarobot_provider.sensors.datarobot import StatusCheckJobCompleteSensor


def test_sensor_autopilot_complete__success(mocker):
    project_mock = mocker.Mock()
    project_mock._autopilot_status_check.return_value = {"autopilot_done": True}
    mocker.patch.object(dr.Project, "get", return_value=project_mock)

    operator = AutopilotCompleteSensor(task_id="check_autopilot_complete", project_id="project-id")
    result = operator.poke(context={})

    assert result is True


def test_sensor_autopilot_complete__fail(mocker):
    project_mock = mocker.Mock()
    project_mock._autopilot_status_check.return_value = {"autopilot_done": False}
    mocker.patch.object(dr.Project, "get", return_value=project_mock)

    operator = AutopilotCompleteSensor(task_id="check_autopilot_complete", project_id="project-id")
    result = operator.poke(context={})

    assert result is False


def test_sensor_scoring_complete__success(mocker):
    job_mock = mocker.Mock()
    job_mock.get_status.return_value = {"status": "COMPLETED"}
    mocker.patch.object(dr.BatchPredictionJob, "get", return_value=job_mock)

    operator = ScoringCompleteSensor(task_id="check_scoring_complete", job_id="job-id")
    result = operator.poke(context={})

    assert result is True


def test_sensor_scoring_complete__fail(mocker):
    job_mock = mocker.Mock()
    job_mock.get_status.return_value = {"status": "UNKNOWN_STATUS"}
    mocker.patch.object(dr.BatchPredictionJob, "get", return_value=job_mock)

    operator = ScoringCompleteSensor(task_id="check_scoring_complete", job_id="job-id")
    result = operator.poke(context={})

    assert result is False


def test_sensor_scoring_complete__raise_error(mocker):
    job_mock = mocker.Mock()
    job_mock.get_status.return_value = {"status": "ABORT"}
    mocker.patch.object(dr.BatchPredictionJob, "get", return_value=job_mock)

    operator = ScoringCompleteSensor(task_id="check_autopilot_complete", job_id="job-id")

    with pytest.raises(AsyncProcessUnsuccessfulError):
        operator.poke(context={})


@pytest.mark.parametrize(
    "status, expected",
    [
        ("COMPLETED", True),
        ("UNKNOWN_STATUS", False),
    ],
)
def test_status_check_complete(mocker, status, expected):
    job_mock = mocker.Mock()
    job_status_result = JobStatusResult(
        status=status,
        status_id="job-id",
        completed_resource_url="completed-resource-url",
        message="message",
    )
    job_mock.get_status.return_value = job_status_result
    mocker.patch.object(StatusCheckJob, "from_id", return_value=job_mock)

    operator = StatusCheckJobCompleteSensor(task_id="check_scoring_complete", job_id="job-id")
    result = operator.poke(context={})

    assert result is expected


@pytest.mark.parametrize(
    "status",
    [
        "ABORT",
        "ERROR",
    ],
)
def test_status_check_complete_errors(mocker, status):
    job_mock = mocker.Mock()
    job_status_result = JobStatusResult(
        status=status,
        status_id="job-id",
        completed_resource_url="completed-resource-url",
        message="message",
    )
    job_mock.get_status.return_value = job_status_result
    mocker.patch.object(StatusCheckJob, "from_id", return_value=job_mock)

    operator = StatusCheckJobCompleteSensor(task_id="check_scoring_complete", job_id="job-id")
    with pytest.raises(AsyncProcessUnsuccessfulError):
        operator.poke(context={})
