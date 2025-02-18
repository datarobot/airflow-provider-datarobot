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


class TrainModelOperator(BaseDatarobotOperator):
    """
    Submit a job to the queue to train a model from specific blueprint.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param blueprint_id: DataRobot blueprint ID
    :type blueprint_id: str
    :param featurelist_id: The identifier of the featurelist to use.
        If not defined, the default for this project is used.
    :type featurelist_id: str, optional
    :source_project_id: Which project created this blueprint_id.
        If ``None``, it defaults to looking in this project.
        Note that you must have read permissions in this project.
    :source_project_id: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: model training job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "blueprint_id",
        "featurelist_id",
        "source_project_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        blueprint_id: str,
        featurelist_id: Optional[str] = None,
        source_project_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.blueprint_id = blueprint_id
        self.featurelist_id = featurelist_id
        self.source_project_id = source_project_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required.")

        if not self.blueprint_id:
            raise ValueError("blueprint_id is required.")

    def execute(self, context: Context) -> str:
        project: dr.Project = dr.Project.get(self.project_id)
        blueprint = dr.Blueprint.get(self.project_id, self.blueprint_id)

        job_id = project.train(
            blueprint,
            sample_pct=context["params"].get("sample_pct", None),
            featurelist_id=self.featurelist_id,
            source_project_id=self.source_project_id,
            scoring_type=context["params"].get("scoring_type", None),
            training_row_count=context["params"].get("training_row_count", None),
            n_clusters=context["params"].get("n_clusters", None),
        )

        self.log.info(f"Model Training Job submitted job_id={job_id}")

        return job_id


class RetrainModelOperator(BaseDatarobotOperator):
    """
    Submit a job to the queue to retrain a model on a specific sample size and/or custom featurelist
    :param project_id: DataRobot project ID
    :type project_id: str
    :param model_id: DataRobot model ID
    :type model_id: str
    :param featurelist_id: The identifier of the featurelist to use.
        If not defined, the default for this project is used.
    :type featurelist_id: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: model retraining job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
        "featurelist_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        featurelist_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.featurelist_id = featurelist_id

    def validate(self) -> None:
        if self.project_id is None:
            raise ValueError("project_id is required.")

        if self.model_id is None:
            raise ValueError("model_id is required.")

    def execute(self, context: Context) -> str:
        model = dr.Model.get(self.project_id, self.model_id)

        job_id = model.train(
            featurelist_id=self.featurelist_id,
            sample_pct=context["params"].get("sample_pct", None),
            scoring_type=context["params"].get("scoring_type", None),
            training_row_count=context["params"].get("training_row_count", None),
        )

        self.log.info(f"Model Retraining Job submitted job_id={job_id}")

        return job_id
