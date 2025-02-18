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
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator

DEFAULT_MAX_WAIT_SEC = 600


class AddExternalDatasetOperator(BaseDatarobotOperator):
    """
    Upload a new dataset from a catalog dataset to make predictions for a model
    :param project_id: DataRobot project ID
    :type project_id: str
    :param dataset_id: DataRobot AI Catalog dataset ID
    :type dataset_id: str
    :param credential_id: DataRobot Credentials ID
    :type credential_id: str
    :param dataset_version_id: DataRobot AI Catalog dataset version ID
    :type dataset_version_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: External dataset ID for the model
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "dataset_id",
        "credential_id",
        "dataset_version_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        dataset_id: str,
        credential_id: Optional[str] = None,
        dataset_version_id: Optional[str] = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credential_id = credential_id
        self.dataset_version_id = dataset_version_id
        self.max_wait_sec = max_wait_sec

    def validate(self) -> None:
        if self.project_id is None:
            raise ValueError("project_id is required to add external dataset.")

        if self.dataset_id is None:
            raise ValueError("dataset_id is required to add external dataset.")

    def execute(self, context: Context) -> str:
        project = dr.Project.get(self.project_id)

        external_dataset = project.upload_dataset_from_catalog(
            dataset_id=self.dataset_id,
            credential_id=self.credential_id,
            dataset_version_id=self.dataset_version_id,
            max_wait=self.max_wait_sec,
        )

        self.log.info(
            f"External Dataset added to the Project, external dataset_id={external_dataset.id}"
        )

        return external_dataset.id


class RequestModelPredictionsOperator(BaseDatarobotOperator):
    """
    Requests predictions against a previously uploaded dataset.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param model_id: DataRobot model ID
    :type model_id: str
    :param external_dataset_id: DataRobot external dataset ID
    :type external_dataset_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Model predictions job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
        "external_dataset_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        external_dataset_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.external_dataset_id = external_dataset_id

    def validate(self) -> None:
        if self.project_id is None:
            raise ValueError("project_id is required to compute model predictions.")

        if self.model_id is None:
            raise ValueError("model_id is required to compute model predictions.")

        if self.external_dataset_id is None:
            raise ValueError("external_dataset_id is required to compute model predictions.")

    def execute(self, context: Context) -> str:
        model = dr.models.Model.get(self.project_id, self.model_id)

        predict_job = model.request_predictions(dataset_id=self.external_dataset_id)

        self.log.info(f"Model predictions requested, job_id={predict_job.id}")

        return predict_job.id
