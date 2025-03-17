from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from datarobot import QUEUE_STATUS

from datarobot_provider.operators.queue import CancelJobOperator


@pytest.mark.parametrize(
    "project_id, job_id, expected_exception, match",
    [
        ("project-id", "job-id", None, None),
        (None, "job-id", ValueError, "project_id is required."),
        ("project-id", None, ValueError, "job_id is required."),
    ],
)
def test_cancel_job_operator_validate(project_id, job_id, expected_exception, match):
    operator = CancelJobOperator(task_id="cancel_job", project_id=project_id, job_id=job_id)
    if expected_exception:
        with pytest.raises(expected_exception, match=match):
            operator.validate()
    else:
        operator.validate()


@patch("datarobot_provider.operators.queue.dr.Project.get")
@patch("datarobot_provider.operators.queue.dr.Job.get")
def test_cancel_job_operator_execute(mock_get_job, mock_get_project):
    mock_project = MagicMock()
    mock_job = MagicMock()
    mock_job.status = QUEUE_STATUS.INPROGRESS
    mock_get_project.return_value = mock_project
    mock_get_job.return_value = mock_job

    operator = CancelJobOperator(task_id="cancel_job", project_id="project-id", job_id="job-id")
    operator.execute(context={})

    mock_get_project.assert_called_once_with("project-id")
    mock_get_job.assert_called_once_with("project-id", "job-id")
    mock_project.pause_autopilot.assert_called_once()
    mock_job.cancel.assert_called_once()
    mock_project.unpause_autopilot.assert_called_once()


@patch("datarobot_provider.operators.queue.dr.Project.get")
@patch("datarobot_provider.operators.queue.dr.Job.get")
def test_cancel_job_operator_execute_job_completed(mock_get_job, mock_get_project):
    mock_project = MagicMock()
    mock_job = MagicMock()
    mock_job.status = "COMPLETED"
    mock_get_project.return_value = mock_project
    mock_get_job.return_value = mock_job

    operator = CancelJobOperator(task_id="cancel_job", project_id="project-id", job_id="job-id")
    operator.execute(context={})

    mock_get_project.assert_called_once_with("project-id")
    mock_get_job.assert_called_once_with("project-id", "job-id")
    mock_project.pause_autopilot.assert_not_called()
    mock_job.cancel.assert_not_called()
    mock_project.unpause_autopilot.assert_not_called()
