# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import Optional
from typing import TypedDict

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from datarobot._experimental.models.notebooks import Notebook
from datarobot._experimental.models.notebooks.session import StartSessionParameters

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class NotebookParametersData(TypedDict):
    data: list[StartSessionParameters]


class NotebookRunOperator(BaseDatarobotOperator):
    """
    Runs a DataRobot Notebook.

    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param notebook_path: Path to the notebook file. Must be provided if the notebook is part of a Codespace.
    :type notebook_path: str
    :param notebook_parameters: Parameters to be set as environment variables in the notebook session. Must be in the
        form of `{"name": "FOO", "value": "MyValue"}`
    :type notebook_parameters: dict, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: The ID of the triggered notebook run.
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "notebook_id",
        "notebook_path",
        "notebook_parameters",
    ]

    def __init__(
        self,
        *,
        notebook_id: str,
        notebook_path: Optional[str],
        notebook_parameters: Optional[NotebookParametersData] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.notebook_path = notebook_path
        self.notebook_parameters = notebook_parameters

    def execute(self, context: Context) -> str:
        # DAGs using Airflow's `Param` model can't seem to take an array/list - it needs to be an object/dict
        parameters = self.notebook_parameters.get("data") if self.notebook_parameters else None

        # Fetch the notebook
        notebook = Notebook.get(notebook_id=self.notebook_id)

        if not notebook.use_case_id:
            raise AirflowException("Notebook should have use_case_id")

        # Run the notebook
        manual_run = notebook.run(
            notebook_path=self.notebook_path,
            parameters=parameters,
        )
        self.log.info(f"Notebook triggered. Manual run ID: {manual_run.id}")

        return manual_run.id
