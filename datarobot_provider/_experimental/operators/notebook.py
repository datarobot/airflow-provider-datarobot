# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import json
from pathlib import Path
from typing import Any
from typing import Optional
from typing import TypedDict
from typing import Union

from airflow.exceptions import AirflowException
from airflow.utils.context import Context

try:
    from datarobot.models.notebooks import Notebook
    from datarobot.models.notebooks.enums import ManualRunType
    from datarobot.models.notebooks.session import StartSessionParameters
except ImportError:
    from datarobot._experimental.models.notebooks import Notebook
    from datarobot._experimental.models.notebooks.enums import ManualRunType
    from datarobot._experimental.models.notebooks.session import StartSessionParameters

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class NotebookParametersData(TypedDict):
    data: list[StartSessionParameters]


OperatorParametersType = Union[str, NotebookParametersData, None]


class NotebookRunOperator(BaseDatarobotOperator):
    """
    Runs a DataRobot Notebook.

    :param notebook_id: DataRobot notebook ID
    :type notebook_id: str
    :param notebook_path: Path to the notebook file. Must be provided if the notebook is part of a Codespace.
    :type notebook_path: str
    :param notebook_parameters: Parameters to be set as environment variables in the notebook session. Must be in the
        form of `{"data": [{"name": "FOO", "value": "MyValue"}, {"name": "BAR", "value": "OtherValue"}]}`
    :type notebook_parameters: dict, optional
    :return: The ID of the triggered notebook run.
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        "notebook_id",
        "notebook_path",
        "notebook_parameters",
    ]

    def __init__(
        self,
        *,
        notebook_id: str = "{{ params.notebook_id }}",
        notebook_path: Optional[str] = "{{ params.notebook_path | default('') }}",
        notebook_parameters: OperatorParametersType = "{{ params.notebook_parameters | default('') }}",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.notebook_id = notebook_id
        self.notebook_path = notebook_path
        self.notebook_parameters = notebook_parameters
        self.parameters = None

    def _parse_notebook_parameters(self) -> None:
        if self.notebook_parameters:
            if isinstance(self.notebook_parameters, str):
                parsed_parameters = json.loads(self.notebook_parameters)
            else:
                parsed_parameters = self.notebook_parameters
            # DAGs using Airflow's `Param` model can't seem to take an array/list - it needs to be an object/dict
            self.parameters = parsed_parameters.get("data")

    def _validate_notebook_parameters(self) -> None:
        try:
            self._parse_notebook_parameters()
        except Exception as exc:
            self.log.warning(
                f"Error ({str(exc)}) parsing notebook parameters: {self.notebook_parameters}"
                f" type={type(self.notebook_parameters)}"
            )
            raise AirflowException("Please check the format of your notebook parameters.") from exc

    def _validate_notebook_path(self) -> None:
        path_suffix = ".ipynb"
        if self.notebook_path:
            path = Path(self.notebook_path)
            if not path.is_absolute():
                raise AirflowException(
                    f"Supplied notebook path ({self.notebook_path}) must be an absolute path."
                )
            if path.suffix != path_suffix:
                raise AirflowException(
                    f"Supplied notebook path ({self.notebook_path}) must end with '{path_suffix}."
                )

    def validate(self) -> None:
        self._validate_notebook_path()
        self._validate_notebook_parameters()

    def execute(self, context: Context) -> str:
        self._parse_notebook_parameters()

        # Fetch the notebook
        notebook = Notebook.get(notebook_id=self.notebook_id)

        if not notebook.use_case_id:
            raise AirflowException("Notebook should have use_case_id")

        # Run the notebook as a manual run of type "pipeline"
        if hasattr(notebook, "run_as_job"):
            manual_run = notebook.run_as_job(
                notebook_path=self.notebook_path,
                parameters=self.parameters,
                manual_run_type=ManualRunType.PIPELINE,
            )
        else:
            # TODO: [CFX-2798] Remove this once both versions of early-access packages are in sync
            manual_run = notebook.run(
                notebook_path=self.notebook_path,
                parameters=self.parameters,
                manual_run_type=ManualRunType.PIPELINE,
            )
        self.log.info(f"Notebook triggered. Manual run ID: {manual_run.id}")

        return manual_run.id
