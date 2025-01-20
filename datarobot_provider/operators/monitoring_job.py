# Copyright 2022 DataRobot, Inc. and its affiliates.
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
from datarobot import BatchMonitoringJob

from datarobot_provider.hooks.datarobot import DataRobotHook


class BatchMonitoringOperator(BaseOperator):
    """
    Creates a batch monitoring job in DataRobot.
    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datastore_id: DataRobot DataStore ID for jdbc source connection
    :type datastore_id: str
    :param credential_id: DataRobot Credentials ID for source connection
    :type credential_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Batch Monitoring job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "deployment_id",
        "datastore_id",
        "credential_id",
    ]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: Optional[str] = None,
        datastore_id: Optional[str] = None,
        credential_id: Optional[str] = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datastore_id = datastore_id
        self.credential_id = credential_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        monitoring_job_settings = context["params"]["monitoring_settings"]

        # in case of deployment_id was not set from operator argument:
        if self.deployment_id is None:
            self.deployment_id = context["params"]["deployment_id"]

        if self.credential_id is not None:
            monitoring_job_settings["intake_settings"]["credential_id"] = self.credential_id

        intake_settings = monitoring_job_settings.get("intake_settings", dict())

        intake_type = intake_settings.get("type")

        # in case of JDBC intake from operator argument:
        if intake_type == "jdbc" and self.datastore_id is not None:
            monitoring_job_settings["intake_settings"]["data_store_id"] = self.datastore_id

        self.log.info(
            f"Loading monitoring data for deployment_id={self.deployment_id} "
            f"with settings: {monitoring_job_settings}"
        )

        if intake_type == "dataset":
            dataset_id = intake_settings.get("dataset_id")
            if not dataset_id:
                raise ValueError(
                    "Invalid or missing `dataset_id` value for the `dataset` intake type."
                )
            dataset = dr.Dataset.get(dataset_id)
            intake_settings["dataset"] = dataset

            # We no longer need the ID
            del intake_settings["dataset_id"]

        job = BatchMonitoringJob.run(self.deployment_id, **monitoring_job_settings)

        self.log.info(f"Monitoring Job submitted job_id={job.id}")

        return job.id
