# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import List
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from pandas import DataFrame

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class ModelRequestPredictionsOperator(BaseDatarobotOperator):
    """
    Start predictions generation job for a model

    Args:
        project_id (str): DataRobot project ID
        model_id (str): DataRobot model ID
        dataset_id (str, optional): DataRobot dataset ID. Defaults to None.
        file_path (str, optional): File path to the dataset. Defaults to None.

    Returns:
        str - PredictJob ID

    Raises:
        ValueError: If neither dataset_id nor file_path is provided
        ValueError: If both dataset_id and file_path are provided
    """

    template_fields: List[str] = ["project_id", "model_id", "dataset_id", "file_path"]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        dataset_id: Optional[str] = None,
        file_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.dataset_id = dataset_id
        self.file_path = file_path

    def validate(self) -> None:
        if not self.dataset_id and not self.file_path:
            raise AirflowFailException("Either dataset_id or file_path must be provided")
        if self.dataset_id and self.file_path:
            raise AirflowFailException("Both dataset_id and file_path cannot be provided")

    def execute(self, context: Context) -> str:
        model = dr.Model.get(
            project=self.project_id,
            model_id=self.model_id,
        )

        if self.dataset_id:
            self.log.info(
                f"Requesting predictions for model {self.model_id} using dataset {self.dataset_id}"
            )
            predict_job = model.request_predictions(dataset_id=self.dataset_id)
        else:
            self.log.info(
                f"Requesting predictions for model {self.model_id} using file {self.file_path}"
            )
            predict_job = model.request_predictions(file_path=self.file_path)

        self.log.info(f"Model predictions requested, predict_job_id={predict_job.id}")
        return predict_job.id


class PredictJobGetPredictionsOperator(BaseDatarobotOperator):
    """
    Get predictions from a PredictJob
    """

    template_fields: List[str] = ["project_id", "predict_job_id"]

    def __init__(self, *, project_id: str, predict_job_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.predict_job_id = predict_job_id

    def validate(self) -> None:
        if not self.predict_job_id or not self.project_id:
            raise AirflowFailException("predict_job_id and project_id must be provided")

    def execute(self, context: Context) -> DataFrame:
        predictions = dr.PredictJob.get_predictions(
            project_id=self.project_id,
            predict_job_id=self.predict_job_id,
        )

        self.log.info(f"Predictions retrieved, predict_job_id={self.predict_job_id}")

        ## NOTE: Can we even return a DataFrame here?
        return predictions


class PredictJobGetOperator(BaseDatarobotOperator):
    """
    Get a PredictJob
    """

    template_fields: List[str] = ["project_id", "predict_job_id"]

    def __init__(self, *, project_id: str, predict_job_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.predict_job_id = predict_job_id

    def validate(self) -> None:
        if not self.predict_job_id or not self.project_id:
            raise AirflowFailException("predict_job_id and project_id must be provided")

    def execute(self, context: Context) -> dr.PredictJob:
        try:
            predict_job = dr.PredictJob.get(
                project_id=self.project_id,
                predict_job_id=self.predict_job_id,
            )
        except dr.errors.PendingJobFinished:
            # NOTE: Is this the correct exception to raise in this case?
            raise AirflowFailException(
                "PredictJob has already finished, please use PredictJobGetPredictionsOperator to retrieve predictions"
            ) from dr.errors.PendingJobFinished

        self.log.info(f"PredictJob retrieved, predict_job_id={self.predict_job_id}")

        # NOTE: Can we return the PredictJob object here?
        return predict_job


class PredictJobGetResultWhenCompleteOperator(BaseDatarobotOperator):
    """
    Wait and retrieve results of a PredictJob
    """
