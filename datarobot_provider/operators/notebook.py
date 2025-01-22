# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from datarobot._experimental.models.notebooks import Notebook

from datarobot_provider.hooks.datarobot import DataRobotHook

ParametersType = list[dict[str, str]] | None


# TODO: Separate operators for Standalone versus Codespace?
class NotebookOperator(BaseOperator):
    """
    Runs a DataRobot Notebook.
    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param parameters: TODO: Fill in ...
    :type parameters: TODO: Fill in ...
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: TODO ...
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "notebook_id",
    ]

    def __init__(
        self,
        *,
        notebook_id: str,
        parameters: ParametersType = None,
        datarobot_conn_id: str = "datarobot_default",  # TODO: Make this a constant somewhere
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        parameters = context["params"]["notebook_parameters"]["data"]

        # Run the notebook
        self.log.info(f"Trying to run Notebook {self.notebook_id=}")
        self.log.info(f"Using parameters {parameters=}")
        notebook = Notebook.get(notebook_id=self.notebook_id)
        manual_run = notebook.run(parameters=parameters)
        self.log.info(f"Notebook triggered {manual_run=}")

        revision_id = manual_run.wait_for_completion()
        # TODO: Return the revision_id or something else?
        return revision_id
