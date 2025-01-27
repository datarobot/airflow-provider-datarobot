# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import List
from typing import Optional
from typing import TypedDict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context
from datarobot._experimental.models.notebooks.notebook import Notebook
from datarobot._experimental.models.notebooks.session import StartSessionParameters

from datarobot_provider.hooks.datarobot import DataRobotHook


class NotebookParametersData(TypedDict):
    data: List[StartSessionParameters]


class NotebookSessionStartOperator(BaseOperator):
    """
    Starts a DataRobot Notebook session.
    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param notebook_parameters: Parameters to be set as environment variables in the notebook session. Must be in the
        form of `{"name": "FOO", "value": "MyValue"}`
    :type notebook_parameters: dict, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: The notebook session's ID.
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "notebook_id",
        "notebook_parameters",
    ]

    def __init__(
        self,
        *,
        notebook_id: str,
        notebook_parameters: Optional[NotebookParametersData] = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.notebook_parameters = notebook_parameters
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

        self.log.info("Starting session.")
        # DAGs using Airflow's `Param` model can't seem to take an array/list but it needs to be an object/dict
        parameters = self.notebook_parameters.get("data") if self.notebook_parameters else None
        session = notebook.start_session(is_triggered_run=True, parameters=parameters)

        return session.session_id


class NotebookExecuteOperator(BaseOperator):
    """
    Stops a DataRobot Notebook session.
    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: The notebook's ID.
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
        datarobot_conn_id: str = "datarobot_default",
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

        self.log.info(f"Fetching notebook w/ ID: {self.notebook_id}")
        notebook = Notebook.get(self.notebook_id)

        self.log.info("Executing notebook.")
        notebook.execute()

        return self.notebook_id


class NotebookSessionStopOperator(BaseOperator):
    """
    Stops a DataRobot Notebook session.
    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: The notebook session's ID.
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
        datarobot_conn_id: str = "datarobot_default",
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

        self.log.info(f"Fetching notebook w/ ID: {self.notebook_id}")
        notebook = Notebook.get(self.notebook_id)

        self.log.info("Stopping session.")
        session = notebook.stop_session()

        return session.session_id
