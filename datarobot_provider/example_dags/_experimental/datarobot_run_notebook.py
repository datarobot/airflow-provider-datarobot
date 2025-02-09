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
    "notebook_id": "6798e158cc74377e3ceb4868",
    "notebook_path": "/home/notebooks/storage/test.py"
    "notebook_parameters": {
        "data": [{"name": "foo", "value": "bar"}]
    }
}
"""

from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from datarobot_provider._experimental.operators.notebook import NotebookRunOperator
from datarobot_provider._experimental.sensors.notebook import NotebookRunCompleteSensor

# Default to 24 hours
DEFAULT_NOTEBOOK_EXECUTION_TIMEOUT = 1440


@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    tags=["example", "notebook"],
    # Default json config example:
    params={
        "notebook_id": "put_your_notebook_id_here",
        "notebook_path": "put_notebook_path_here_if_codespace",
        "notebook_parameters": Param(
            {"data": [{"name": "foo", "value": "bar"}]}, type=["object", "null"]
        ),
    },
)
def datarobot_notebook_run(
    notebook_id=None,
    notebook_path=None,
    notebook_parameters=None,
):
    # Timeout waiting for complete notebook execution
    execution_timeout = DEFAULT_NOTEBOOK_EXECUTION_TIMEOUT * 60

    run_notebook = NotebookRunOperator(
        task_id="run_notebook",
        notebook_id=notebook_id,
        notebook_path=notebook_path,
        notebook_parameters=notebook_parameters,
    )

    # Status check each 15 sec
    poke_interval = 15
    check_notebook_run_complete = NotebookRunCompleteSensor(
        task_id="check_notebook_run_complete",
        job_id=run_notebook.output,
        poke_interval=poke_interval,
        timeout=execution_timeout,
    )

    run_notebook >> check_notebook_run_complete


datarobot_notebook_run_dag = datarobot_notebook_run()

if __name__ == "__main__":
    datarobot_notebook_run_dag.test()
