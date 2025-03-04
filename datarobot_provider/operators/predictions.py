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
    Starts predictions generation job for a model.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        dataset_id (Optional[str]): DataRobot dataset ID. Defaults to None.
        file_path (Optional[str]): File path to the dataset. Defaults to None.

    Returns:
        str: PredictJob ID.

    Raises:
        AirflowFailException: If neither dataset_id nor file_path is provided.
        AirflowFailException: If both dataset_id and file_path are provided.
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
    Gets predictions from a PredictJob.

    Args:
        project_id (str): DataRobot project ID.
        predict_job_id (str): PredictJob ID.

    Returns:
        DataFrame: Predictions DataFrame.

    Raises:
        AirflowFailException: If predict_job_id or project_id is not provided.
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
    Gets a PredictJob.

    Args:
        project_id (str): DataRobot project ID.
        predict_job_id (str): PredictJob ID.

    Returns:
        dr.PredictJob: PredictJob object.

    Raises:
        AirflowFailException: If predict_job_id or project_id is not provided.
        AirflowFailException: If PredictJob has already finished.
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
    Waits and retrieves results of a PredictJob.

    Args:
        project_id (str): DataRobot project ID.
        predict_job_id (str): PredictJob ID.

    Returns:
        DataFrame: Predictions DataFrame.

    Raises:
        AirflowFailException: If predict_job_id or project_id is not provided.
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
        predict_job = dr.PredictJob.get(
            project_id=self.project_id,
            predict_job_id=self.predict_job_id,
        )

        predictions = predict_job.get_result_when_complete()

        self.log.info(f"Predictions waited for and retrieved, predict_job_id={self.predict_job_id}")

        # NOTE: Again, can we return a DataFrame here?
        return predictions
