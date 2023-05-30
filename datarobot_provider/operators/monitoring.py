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
from typing import List

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%s"


class GetServiceStatsOperator(BaseOperator):
    """
    Gets service stats measurements from a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Service stats for a Deployment
    :rtype: List[dict]
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = ["deployment_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
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

    def execute(self, context: Dict[str, Any]) -> List[dict]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Getting service stats for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        service_stats_params = context["params"].get("service_stats", {})
        service_stats = deployment.get_service_stats(**service_stats_params)
        return _serialize_service_stats(service_stats)


def _serialize_service_stats(service_stats_obj, date_format=DATETIME_FORMAT):
    service_stats_dict = service_stats_obj.__dict__.copy()
    service_stats_dict["period"] = {
        "start": service_stats_obj.period["start"].strftime(date_format),
        "end": service_stats_obj.period["end"].strftime(date_format),
    }
    return service_stats_dict
