# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import List
from typing import Optional
from typing import Union

import datarobot as dr
import pandas as pd
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class SubmitActualsFromCatalogOperator(BaseDatarobotOperator):
    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "deployment_id",
        "dataset_id",
        "dataset_version_id",
    ]

    def __init__(
        self,
        *,
        deployment_id: str,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.dataset_id = dataset_id
        self.dataset_version_id = dataset_version_id

    def validate(self) -> None:
        if self.deployment_id is None:
            raise ValueError("deployment_id is required to submit actuals.")

        if self.dataset_id is None:
            raise ValueError("dataset_id is required to submit actuals.")

    def execute(self, context: Context) -> str:
        self.log.info("Uploading Actuals from AI Catalog")

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


class SubmitActualsOperator(BaseDatarobotOperator):
    """
    ability to upload actuals, will be used to calculate accuracy metrics.

    This operator gives you ability to submit actuals
    DataRobot's `Deployment.submit_actuals()` method. It allows
    optional extra parameters to be passed to the DataRobot client call.

    Args:
        deployment_id (str): DataRobot deployment ID.
        data (Union[pd.DataFrame, List]): list or pandas.DataFrame
            Defaults to an empty list if not provided.
        batch_size (int): The max number of actuals in each request. Defaults to 10000.
        kwargs (dict): Additional keyword arguments passed to the BaseDatarobotOperator.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "deployment_id",
        "data",
        "batch_size",
    ]

    def __init__(
        self,
        *,
        deployment_id: str,
        data: Union[pd.DataFrame, List],
        batch_size: int = 10000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.data = data
        self.batch_size = batch_size

    def validate(self) -> None:
        if self.deployment_id is None:
            raise ValueError("deployment_id is required to submit actuals.")

        if not isinstance(self.data, (list, pd.DataFrame)):
            raise ValueError(
                "data should be either a list of dict-like objects or a pandas.DataFrame"
            )

    def execute(self, context: Context) -> None:
        self.log.info("Uploading Actuals")

        deployment = dr.Deployment(id=self.deployment_id)

        deployment.submit_actuals(batch_size=self.batch_size, data=self.data)

        self.log.debug(f"Uploading Actuals for deployment: {self.deployment_id}")
