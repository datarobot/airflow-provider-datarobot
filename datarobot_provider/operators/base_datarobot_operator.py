# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from typing import Any, Callable, Iterable
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook


class BaseDatarobotOperator(BaseOperator):
    ui_color = "#f4a460"

    dr_hook: DataRobotHook
    min_version: Optional[str] = None
    requires_early_access = False

    def pre_execute(self, context: Context):
        super().pre_execute(context)

        self.dr_hook = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id)
        self.dr_hook.run()

        self.validate()

    def validate(self):
        """Implement your validation of rendered operator fields here."""

    def __init__(self, *, datarobot_conn_id: str = "datarobot_default", **kwargs: Any):
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )


class DatarobotMethodOperator(BaseDatarobotOperator):
    method: Callable
    return_field = 'id'
    required_params: List[str]

    @classmethod
    def __init_subclass__(cls):
        super().__init_subclass__(cls)
        if not hasattr(cls, 'method'):
            raise ValueError(f'*method* field must be defined in {cls.__name__}')

        cls.__doc__ = cls.method.__doc__

    @property
    def method_params(self) -> Iterable[str]:
        return self.method.__annotations__.keys()

    @classmethod
    def get_non_default_params(cls) -> List[str]:
        return [
            param.name
            for param in
            inspect.signature(cls.method).parameters.values()
            if param.default is inspect.Parameter.empty
        ]

    @classmethod
    def get_not_none_params(cls) -> List[str]:
        if hasattr(cls, 'required_params'):
            return cls.required_params

        return [
            param.name
            for param in
            inspect.signature(cls.method).parameters.values()
            if (
                param.default is inspect.Parameter.empty
                and not (
                    get_origin(param.annotation) is Union
                    and None in get_args(param.annotation)
                )
            )
        ]

    def __init__(self, task_id: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)

        for param in self.method_params:
            if param in kwargs:
                setattr(self, param, kwargs[param])

        if missing := self._get_missing_params():
            raise AirflowException(
                'Following parameters are missing in the task '
                f'"{task_id}" <{self.__class__.__name__}>: [{",".join(missing)}]'
            )

    def _get_missing_params(self) -> List[str]:
        """Get required params not defined in the operator."""
        return [param for param in self.get_non_default_params() if not hasattr(self, param)]

    def validate(self):
        for required_param in self.get_not_none_params():
            if getattr(self, required_param) is None or getattr(self, required_param) == '':
                raise AirflowException(f"{required_param} can't be None.")

    def execute(self, context: Context):
        return self.post_process(
            self.method(**self._get_kwargs())
        )

    def _get_kwargs(self) -> dict:
        return {
            x: getattr(self, x)
            for x in self.method_params
            if hasattr(self, x)
        }

    def post_process(self, method_output):
        if self.return_field is None:
            return method_output

        return getattr(method_output, self.return_field)
