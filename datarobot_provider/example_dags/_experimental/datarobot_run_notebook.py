# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag to run a Codespace notebook with notebook parameters:
{
    "notebook_id": "6798e158cc74377e3ceb4868",
    "notebook_path": "/home/notebooks/storage/test.py"
    "notebook_parameters": {
        "data": [{"name": "foo", "value": "bar"}]
    }
}
"""

from airflow.decorators import dag
from airflow.models.param import Param

from datarobot_provider._experimental.operators.notebook import NotebookRunOperator
from datarobot_provider._experimental.sensors.notebook import NotebookRunCompleteSensor

# Default to 24 hours
DEFAULT_NOTEBOOK_EXECUTION_TIMEOUT = 1440


@dag(
    schedule=None,
    description="Run a DataRobot notebook to completion and store a revision after it completes.",
    dag_display_name="Run DataRobot Notebook",
    tags=["example", "notebook"],
    params={
        "notebook_id": "put_your_notebook_id_here",
        "notebook_path": Param(None, type=["string", "null"]),
        "notebook_parameters": Param(None, type=["object", "null"]),
    },
    render_template_as_native_obj=True,
)
def datarobot_notebook_run():
    # Timeout waiting for complete notebook execution
    execution_timeout = DEFAULT_NOTEBOOK_EXECUTION_TIMEOUT * 60

    run_notebook = NotebookRunOperator(task_id="run_notebook")

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
