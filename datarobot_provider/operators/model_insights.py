# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot.insights import ShapImpact
from datarobot.insights import ShapPreview
from datarobot.models import StatusCheckJob

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class ComputeFeatureImpactOperator(BaseDatarobotOperator):
    """
    Creates Feature Impact job in DataRobot.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Feature Impact job ID.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required to compute Feature Impact.")

        if not self.model_id:
            raise ValueError("model_id is required to compute Feature Impact.")

    def execute(self, context: Context) -> str:
        model = dr.models.Model.get(self.project_id, self.model_id)

        job = model.request_feature_impact()

        self.log.info(f"Feature Impact Job submitted, job_id={job.id}")

        return job.id


class ComputeFeatureEffectsOperator(BaseDatarobotOperator):
    """
    Submit request to compute Feature Effects for the model.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Feature Effects job ID.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required to compute Feature Effects.")

        if not self.model_id:
            raise ValueError("model_id is required to compute Feature Effects.")

    def execute(self, context: Context) -> str:
        model = dr.models.Model.get(self.project_id, self.model_id)

        job = model.request_feature_effect()

        self.log.info(f"Feature Effects Job submitted, job_id={job.id}")

        return job.id


class ComputeShapPreviewOperator(BaseDatarobotOperator):
    """
    Creates SHAP preview job in DataRobot.

    Parameters
    ----------
    model_id : str
        DataRobot model ID
    datarobot_conn_id : str, optional
        Connection ID, defaults to `datarobot_default`

    Returns
    -------
    str
        SHAP preview result id
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "model_id",
    ]

    def __init__(
        self,
        *,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = str(model_id)

    def validate(self) -> None:
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def execute(self, context: Context) -> str:
        job: StatusCheckJob = ShapPreview.compute(entity_id=self.model_id)
        return job.job_id


class ComputeShapImpactOperator(BaseDatarobotOperator):
    """
    Creates SHAP impact job in DataRobot.

    Parameters
    ----------
    model_id : str
        DataRobot model ID
    datarobot_conn_id : str, optional
        Connection ID, defaults to `datarobot_default`

    Returns
    -------
    str
        SHAP impact job id
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "model_id",
    ]

    def __init__(
        self,
        *,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = str(model_id)

    def validate(self) -> None:
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def execute(self, context: Context) -> str:
        job: StatusCheckJob = ShapImpact.compute(entity_id=self.model_id)
        return job.job_id
