# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from typing import Any
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook


class BaseDatarobotOperator(BaseOperator):
    ui_color = "#f4a460"

    dr_hook: DataRobotHook
    min_version: Optional[str] = None
    requires_early_access = False

    def pre_execute(self, context: Any):
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
