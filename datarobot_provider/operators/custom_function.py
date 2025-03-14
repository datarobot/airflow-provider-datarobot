# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import inspect
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class CustomFunctionOperator(BaseDatarobotOperator):
    """
    Executes a custom function and returns its result.

    Args:
        custom_function (int): Custom function to execute
        params (dict): Keyword arguments to pass to the custom function

    Returns:
        any: Result of the custom function execution

    Raises:
        AirflowFailException: If custom_function is not provided
    """

    template_fields: List[str] = ["func_params"]

    def __init__(
        self,
        *,
        custom_func: Callable,
        func_params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_func = custom_func
        self.func_params = func_params or {}

    def execute(self, context: Context) -> Any:
        self.log.info(f"Function execution parameters: {self.func_params}")
        if "log" in inspect.signature(self.custom_func).parameters:
            self.func_params["log"] = self.log
        result = self.custom_func(**self.func_params)
        self.log.info(f"Function execution result: {result}")
        return result
