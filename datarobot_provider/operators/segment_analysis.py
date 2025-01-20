# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook

if TYPE_CHECKING:
    from datarobot.models.deployment.deployment import SegmentAnalysisSettings


class GetSegmentAnalysisSettingsOperator(BaseOperator):
    """
    Get segment analysis settings for a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> "SegmentAnalysisSettings":
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Get Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        segment_analysis_settings = deployment.get_segment_analysis_settings()

        return segment_analysis_settings


class UpdateSegmentAnalysisSettingsOperator(BaseOperator):
    """
    Updates segment analysis settings for a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> None:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Getting Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        current_segment_analysis_settings = deployment.get_segment_analysis_settings()

        segment_analysis_enabled = context["params"].get(
            "segment_analysis_enabled", current_segment_analysis_settings["enabled"]
        )
        segment_analysis_attributes = context["params"].get(
            "segment_analysis_attributes", current_segment_analysis_settings["attributes"]
        )

        if (segment_analysis_enabled != current_segment_analysis_settings["enabled"]) or (
            segment_analysis_attributes != current_segment_analysis_settings["attributes"]
        ):
            self.log.debug(
                f"Trying to update segment analysis settings for deployment_id={self.deployment_id}"
            )
            deployment.update_segment_analysis_settings(
                segment_analysis_enabled=segment_analysis_enabled,
                segment_analysis_attributes=segment_analysis_attributes,
            )
            self.log.info(
                f"Deployment segment analysis settings updated for deployment_id={self.deployment_id}"
            )
        else:
            self.log.info(
                f"No need to update segment analysis settings for deployment_id={self.deployment_id}"
            )
