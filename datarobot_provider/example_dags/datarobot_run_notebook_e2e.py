# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag:
{
    "notebook_id": "put_your_notebook_id_here",
    "notebook_parameters": {
        "data": [{"name": "foo", "value": "bar"}]
    }
}
"""

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from datarobot_provider.operators.notebooks import NotebookExecuteOperator
from datarobot_provider.operators.notebooks import NotebookRevisionCreateOperator
from datarobot_provider.operators.notebooks import NotebookSessionStartOperator
from datarobot_provider.operators.notebooks import NotebookSessionStopOperator
from datarobot_provider.sensors.notebooks import NotebookExecutionCompleteSensor
from datarobot_provider.sensors.notebooks import NotebookSessionRunningSensor

# Default to 24 hours
DEFAULT_NOTEBOOK_EXECUTION_TIMEOUT = 1440


@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    tags=["example", "notebook"],
    # Default json config example:
    params={
        "notebook_id": "put_your_notebook_id_here",
        "notebook_parameters": Param(
            {"data": [{"name": "foo", "value": "bar"}]}, type=["object", "null"]
        ),
    },
)
def datarobot_notebook_connect(
    notebook_id=None,
    notebook_parameters=None,
):
    # Timeout waiting for complete notebook execution
    execution_timeout = DEFAULT_NOTEBOOK_EXECUTION_TIMEOUT * 60

    start_notebook_session = NotebookSessionStartOperator(
        task_id="start_notebook_session",
        notebook_id=notebook_id,
        notebook_parameters=notebook_parameters,
    )

    poke_interval = 15  # Status check each 15 sec
    session_timeout = 15 * 60  # Timeout after waiting 15 minutes for session to start
    check_notebook_session_running = NotebookSessionRunningSensor(
        task_id="check_notebook_session_running",
        notebook_id=notebook_id,
        poke_interval=poke_interval,
        timeout=session_timeout,
    )

    execute_notebook = NotebookExecuteOperator(
        task_id="execute_notebook",
        notebook_id=notebook_id,
    )

    poke_interval = 15  # Status check each 15 sec
    check_notebook_execution_complete = NotebookExecutionCompleteSensor(
        task_id="check_notebook_execution_complete",
        notebook_id=notebook_id,
        poke_interval=poke_interval,
        timeout=execution_timeout,
    )

    notebook_create_revision = NotebookRevisionCreateOperator(
        task_id="notebook_create_revision", notebook_id=notebook_id
    )

    stop_notebook_session = NotebookSessionStopOperator(
        task_id="stop_notebook_session", notebook_id=notebook_id
    )

    (
        start_notebook_session
        >> check_notebook_session_running
        >> execute_notebook
        >> check_notebook_execution_complete
        >> notebook_create_revision
        >> stop_notebook_session
    )


datarobot_notebook_connection_dag = datarobot_notebook_connect()

if __name__ == "__main__":
    datarobot_notebook_connection_dag.test()
