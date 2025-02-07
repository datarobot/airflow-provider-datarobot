# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from typing import Any
from typing import Optional
from typing import Union

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook

XCOM_DEFAULT_USE_CASE_ID = "default_use_case_id"


class BaseDatarobotOperator(BaseOperator):
    ui_color = "#f4a460"

    dr_hook: DataRobotHook

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


class BaseUseCaseEntityOperator(BaseDatarobotOperator):
    def __init__(
        self, *, use_case_id: Optional[str] = "{{ params.use_case_id | default('') }}", **kwargs
    ):
        super().__init__(**kwargs)

        if "use_case_id" not in self.template_fields:
            raise AirflowException(
                f"You must add use_case_id into {self.__class__.__name__} template_fields list "
                "in order to use BaseUseCaseEntityOperator"
            )

        self.use_case_id = use_case_id

    def get_use_case(self, context: Context, required=False) -> Optional[dr.UseCase]:
        """Get a `dr.UseCase` entity based on self.use_case_id or a default Use Case defined at runtime.
        Raises an exception if no Use Case is defined and required=True
        """
        if use_case_id := self.get_use_case_id(context, required=required):
            return dr.UseCase.get(use_case_id)

        return None

    def get_use_case_id(self, context: Context, required=False) -> Optional[str]:
        """Get self.use_case_id or a default Use Case id defined at runtime.
        Raises an exception if no Use Case id is defined and required=True"""
        if use_case_id := (self.use_case_id or self.xcom_pull(context, key=XCOM_DEFAULT_USE_CASE_ID)):
            return use_case_id

        if required:
            raise AirflowException(
                f"{self.__class__.__name__} requires a Use Case. Please define one of:\n"
                "*use_case_id* parameter in the operator\n"
                "*use_case_id* DAG context parameter\n"
                "`GetOrCreateUseCaseOperator(..., set_default=True)` as one of the previous DAG tasks"
            )

        return None

    def add_into_use_case(
        self,
        entity: Union[dr.Project, dr.Dataset, dr.models.Recipe, dr.Application],
        *,
        context: Context,
    ):
        """Add an *entity* into Use Case defined as self.use_case_id or a default DAG Use Case."""
        if use_case := self.get_use_case(context):
            use_case.add(entity)
            self.log.info(
                '%s is added into use case "%s"', entity.__class__.__name__, use_case.name
            )

        else:
            self.log.info("The %s won't be added into any use case.", entity.__class__.__name__)
