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


class PredictionExplanationsInitializationOperator(BaseDatarobotOperator):
    """
    Triggering a prediction explanations initialization of a model.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param model_id: DataRobot model ID
    :type model_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Prediction Explanations Initialization job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: Optional[str] = None,
        model_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id

    def validate(self) -> None:
        if self.project_id is None:
            raise ValueError(
                "project_id is required to trigger a prediction explanations initialization."
            )

        if self.model_id is None:
            raise ValueError(
                "model_id is required to trigger a prediction explanations initialization."
            )

    def execute(self, context: Context) -> str:
        # Initialize prediction explanations
        pei_job = dr.PredictionExplanationsInitialization.create(self.project_id, self.model_id)

        self.log.info(
            f"Triggered prediction explanations initialization of a model, job_id={pei_job.id}"
        )

        return pei_job.id


class ComputePredictionExplanationsOperator(BaseDatarobotOperator):
    """
    Create prediction explanations for the specified dataset.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param model_id: DataRobot model ID
    :type model_id: str
    :param external_dataset_id: DataRobot external dataset ID
    :type external_dataset_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Prediction Explanations Initialization job ID
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
        project_id: Optional[str] = None,
        model_id: Optional[str] = None,
        external_dataset_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.external_dataset_id = external_dataset_id

    def validate(self) -> None:
        if self.project_id is None:
            raise ValueError("project_id is required to compute prediction explanations.")

        if self.model_id is None:
            raise ValueError("model_id is required to compute prediction explanations.")

        if self.external_dataset_id is None:
            raise ValueError("external_dataset_id is required to compute prediction explanations.")

    def execute(self, context: Context) -> str:
        # Creating compute prediction explanations job:
        pe_job = dr.PredictionExplanations.create(
            self.project_id,
            self.model_id,
            self.external_dataset_id,
            max_explanations=context["params"].get("max_explanations", None),
            threshold_low=context["params"].get("threshold_low", None),
            threshold_high=context["params"].get("threshold_high", None),
        )

        self.log.info(
            f"Triggered prediction explanations for the specified dataset, job_id={pe_job.id}"
        )

        return pe_job.id
