# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook

DATAROBOT_MAX_WAIT = 600


class StartAutopilotOperator(BaseOperator):
    """
    Triggers DataRobot Autopilot to train set of models.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param featurelist_id: Specifies which feature list to use.
    :type featurelist_id: str, optional
    :param relationships_configuration_id: ID of the relationships configuration to use.
    :type relationships_configuration_id: str, optional
    :param segmentation_task_id: The segmentation task that should be used to split the project
            for segmented modeling.
    :type segmentation_task_id: str, optional
    :param max_wait_sec: For some settings, an asynchronous task must be run to analyze the dataset.  max_wait
            governs the maximum time (in seconds) to wait before giving up.
    :type max_wait_sec: int, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "featurelist_id",
        "relationships_configuration_id",
        "segmentation_task_id",
    ]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        project_id: str,
        featurelist_id: Optional[str] = None,
        relationships_configuration_id: Optional[str] = None,
        segmentation_task_id: Optional[str] = None,
        max_wait_sec: int = DATAROBOT_MAX_WAIT,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.featurelist_id = featurelist_id
        self.relationships_configuration_id = relationships_configuration_id
        self.segmentation_task_id = segmentation_task_id
        self.max_wait_sec = max_wait_sec
        self.datarobot_conn_id = datarobot_conn_id

        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> None:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()
        # Train models
        project = dr.Project.get(self.project_id)
        if project.target:
            raise ValueError(f"Models are already trained for project_id={project.id}")

        autopilot_settings = context["params"]["autopilot_settings"]

        self.log.info(
            f"Starting DataRobot Autopilot for project_id={project.id} "
            f"with settings={autopilot_settings}"
        )

        if self.featurelist_id:
            autopilot_settings["featurelist_id"] = self.featurelist_id

        if self.relationships_configuration_id:
            autopilot_settings["relationships_configuration_id"] = (
                self.relationships_configuration_id
            )

        if self.segmentation_task_id:
            autopilot_settings["segmentation_task_id"] = self.segmentation_task_id

        autopilot_settings["max_wait"] = self.max_wait_sec

        if (
            "datetime_partitioning_settings" in context["params"]
            and "partitioning_settings" in context["params"]
        ):
            raise ValueError(
                "parameters: datetime_partitioning_settings and partitioning_settings are mutually exclusive"
            )

        if "datetime_partitioning_settings" in context["params"]:
            project.set_datetime_partitioning(**context["params"]["datetime_partitioning_settings"])
        elif "partitioning_settings" in context["params"]:
            project.set_partitioning_method(**context["params"]["partitioning_settings"])

        if "advanced_options" in context["params"]:
            project.set_options(**context["params"]["advanced_options"])

        # finalize the project and start the autopilot
        project.analyze_and_model(**autopilot_settings)
