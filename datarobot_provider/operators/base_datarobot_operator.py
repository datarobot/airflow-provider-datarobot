# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import logging
from typing import Any
from typing import Optional
from typing import Union

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.constants import DATAROBOT_CONN_ID
from datarobot_provider.constants import XCOM_DEFAULT_USE_CASE_ID
from datarobot_provider.hooks.datarobot import DataRobotHook

logger = logging.getLogger(__name__)


class BaseDatarobotOperator(BaseOperator):
    ui_color = "#f4a460"

    dr_hook: DataRobotHook

    def pre_execute(self, context: Context) -> None:
        super().pre_execute(context)

        self.dr_hook = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id)
        self.dr_hook.run()

        self.validate()

    def validate(self) -> None:
        """Implement your validation of rendered operator fields here."""

    def __init__(self, *, datarobot_conn_id: str = DATAROBOT_CONN_ID, **kwargs: Any):
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )


class BaseUseCaseEntityOperator(BaseDatarobotOperator):
    """Base class for all operators which create a Use Case assets,
    i.e. dataset, recipe, project, deployment, notebook, application, etc.

    It introduces *use_case_id* template parameter and helper methods
    to use a default Use Case if no Use Case was defined in the operator itself or a DAG context.
    """

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "use_case_id" not in cls.template_fields:
            cls.template_fields = (*cls.template_fields, "use_case_id")
            logger.info(
                "use_case_id is implicitly added into %s template_fields list.", cls.__name__
            )

    def __init__(
        self,
        *,
        use_case_id: Optional[str] = "{{ params.use_case_id | default('') }}",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.use_case_id = use_case_id

    def get_use_case(self, context: Context, required: bool = False) -> Optional[dr.UseCase]:
        """Get a `dr.UseCase` entity based on self.use_case_id or a default Use Case defined at runtime.
        Raises an exception if no Use Case is defined and required=True
        """
        if use_case_id := self.get_use_case_id(context, required=required):
            return dr.UseCase.get(use_case_id)

        return None

    def get_use_case_id(self, context: Context, required: bool = False) -> Optional[str]:
        """Get self.use_case_id or a default Use Case id defined at runtime.
        Raises an exception if no Use Case id is defined and required=True"""
        if use_case_id := (
            self.use_case_id or self.xcom_pull(context, key=XCOM_DEFAULT_USE_CASE_ID)
        ):
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
    ) -> None:
        """Add an *entity* into Use Case defined as self.use_case_id or a default DAG Use Case."""
        if use_case := self.get_use_case(context):
            use_case.add(entity)
            self.log.info(
                '%s is added into use case "%s"', entity.__class__.__name__, use_case.name
            )

        else:
            self.log.info("The %s won't be added into any use case.", entity.__class__.__name__)
