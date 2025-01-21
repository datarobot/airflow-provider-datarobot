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


class SubmitActualsFromCatalogOperator(BaseOperator):
    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "deployment_id",
        "dataset_id",
        "dataset_version_id",
    ]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.dataset_id = dataset_id
        self.dataset_version_id = dataset_version_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info("Uploading Actuals from AI Catalog")

        if self.deployment_id is None:
            raise ValueError("deployment_id is required to submit actuals.")

        if self.dataset_id is None:
            raise ValueError("dataset_id is required to submit actuals.")

        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        status_job = deployment.submit_actuals_from_catalog_async(
            dataset_id=self.dataset_id,
            dataset_version_id=self.dataset_version_id,
            actual_value_column=context["params"].get("actual_value_column", None),
            association_id_column=context["params"].get("association_id_column", None),
            timestamp_column=context["params"].get("timestamp_column", None),
            was_acted_on_column=context["params"].get("was_acted_on_column", None),
        )

        self.log.debug(f"Uploading Actuals from AI Catalog, job_id: {status_job.job_id}")

        return status_job.job_id
