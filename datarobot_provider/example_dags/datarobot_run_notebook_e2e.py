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
}
"""

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from datarobot_provider.operators.notebooks.revision import NotebookRevisionCreateOperator
from datarobot_provider.sensors.notebooks import NotebookExecutionCompleteSensor
from datarobot_provider.sensors.notebooks import NotebookSessionRunningSensor


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
def datarobot_notebook_connect(notebook_id=None, notebook_parameters=None):
    # Op - start notebook session

    check_notebook_session_running = NotebookSessionRunningSensor(
        task_id="check_notebook_session_running",
        notebook_id=notebook_id,
        poke_interval=15,  # status check each 15 sec
        timeout=900,  # timeout after 15min (15*60sec = 900 sec)
    )

    # Op - execute notebook

    check_notebook_execution_complete = NotebookExecutionCompleteSensor(
        task_id="check_notebook_execution_complete",
        notebook_id=notebook_id,
        poke_interval=15,  # status check each 15 sec
        # TODO: Update this timeout to something more like 24 hours
        timeout=600,  # timeout after 10min (10*60sec = 600 sec)
    )

    notebook_create_revision = NotebookRevisionCreateOperator(
        task_id="notebook_create_revision",
        notebook_id=notebook_id,
    )

    # Op - stop notebook

    check_notebook_session_running >> check_notebook_execution_complete >> notebook_create_revision


datarobot_notebook_connection_dag = datarobot_notebook_connect()

if __name__ == "__main__":
    datarobot_notebook_connection_dag.test()
