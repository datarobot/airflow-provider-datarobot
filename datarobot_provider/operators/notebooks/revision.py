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

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from datarobot._experimental.models.notebooks.notebook import Notebook

from datarobot_provider.hooks.datarobot import DataRobotHook


class NotebookRevisionCreateOperator(BaseOperator):
    """
    Runs a DataRobot Notebook.
    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param name: Name for notebook revision.
    :type name: str, optional
    :param notebook_path: Path to notebook file. Required for Codespace notebooks.
    :type notebook_path: str, optional
    :param is_auto: Signifies if the revision was created from interactive user action. Defaults to `True`.
    :type is_auto: bool, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: The notebook revision's ID.
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "notebook_id",
        "name",
        "notebook_path",
        "is_auto",
    ]

    def __init__(
        self,
        *,
        notebook_id: str,
        name: Optional[str] = None,
        notebook_path: Optional[str] = None,
        is_auto: bool = True,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.name = name
        self.notebook_path = notebook_path
        self.is_auto = is_auto
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Fetching notebook w/ ID: {self.notebook_id}")
        notebook = Notebook.get(self.notebook_id)
        if notebook.is_codespace and not self.notebook_path:
            raise AirflowException(
                "Creating a notebook revision for a Codespace requires `notebook_path`."
            )

        self.log.info("Creating revision.")
        revision = notebook.create_revision(
            name=self.name,
            notebook_path=self.notebook_path,
            is_auto=self.is_auto,
        )
        self.log.info(f"Revision created: {revision}")

        return revision.revision_id
