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

from datarobot_provider.operators.notebook import NotebookOperator


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
    notebook_connect_op = NotebookOperator(
        task_id="notebook_running",
        notebook_id=notebook_id,
        parameters=notebook_parameters,
    )

    notebook_connect_op


datarobot_notebook_connection_dag = datarobot_notebook_connect()

if __name__ == "__main__":
    datarobot_notebook_connection_dag.test()
