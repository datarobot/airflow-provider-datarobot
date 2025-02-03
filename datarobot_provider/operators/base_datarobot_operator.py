# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from typing import Optional, Any

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from packaging.version import Version

from datarobot_provider.hooks.datarobot import DataRobotHook


class BaseDatarobotOperator(BaseOperator):
    ui_color = "#f4a460"

    dr_hook: DataRobotHook
    min_version: Optional[str] = None
    requires_early_access = False

    def pre_execute(self, context: Any):
        super().pre_execute(context)

        self.check_dr_client_version()
        self.dr_hook = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id)
        self.dr_hook.run()

        self.validate()

    def check_dr_client_version(self):
        if self.requires_early_access and not hasattr(dr, '_experimental'):
            package_to_install = 'datarobot-early-access'
            if self.min_version:
                package_to_install = f'{package_to_install}>={self.min_version}'

            raise AirflowException(
                f'{self.__class__.__name__} requires datarobot-early-access package to run. '
                f'Please install it with: pip install {package_to_install}'
            )

        if self.min_version and Version(dr.__version__) < Version(self.min_version):
            raise AirflowException(
                f"{self.__class__.__name__} requires datarobot>={self.min_version} "
                f"Please install it with: pip install datarobot>={self.min_version}"
            )

    def validate(self):
        """Implement your validation of rendered operator fields here."""

    def __init__(self, *, datarobot_conn_id: str = "datarobot_default", **kwargs: Any):
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )