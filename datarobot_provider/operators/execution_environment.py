# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict
from typing import Iterable

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from datarobot.models.execution_environment import RequiredMetadataKey

from datarobot_provider.hooks.datarobot import DataRobotHook

DEFAULT_MAX_WAIT_SEC = 600


class CreateExecutionEnvironmentOperator(BaseOperator):
    """
    Create an execution environment.
    :param name: execution environment name
    :type name: str
    :param description: execution environment description
    :type description: str, optional
    :param programming_language: programming language of the environment to be created.
        Can be "python", "r", "java" or "other". Default value - "other"
    :type programming_language: str, optional
    :return: execution environment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "name",
        "description",
        "programming_language",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        name: str = None,
        description: str = None,
        programming_language: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.programming_language = programming_language
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        required_metadata_keys = context["params"].get("required_metadata_keys", None)
        metadata_keys = []
        if required_metadata_keys:
            metadata_keys = [
                RequiredMetadataKey(field_name=key["field_name"], display_name=key["display_name"])
                for key in required_metadata_keys
            ]

        execution_environment_name = (
            context["params"].get("execution_environment_name", None)
            if self.name is None
            else self.name
        )
        execution_environment_description = (
            context["params"].get("execution_environment_description", None)
            if self.description is None
            else self.description
        )
        programming_language = (
            context["params"].get("programming_language", None)
            if self.programming_language is None
            else self.programming_language
        )

        execution_environment = dr.ExecutionEnvironment.create(
            name=execution_environment_name,
            description=execution_environment_description,
            programming_language=programming_language,
            required_metadata_keys=metadata_keys,
        )

        self.log.info(
            f"Execution environment created, execution_environment_id={execution_environment.id}"
        )

        return execution_environment.id
