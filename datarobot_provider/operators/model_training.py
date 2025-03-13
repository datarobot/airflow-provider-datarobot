# Copyright 2022 DataRobot, Inc. and its affiliates.
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
from typing import Tuple

import datarobot as dr
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class TrainModelOperator(BaseDatarobotOperator):
    """
    Submit a job to the queue to train a model from a specific blueprint.

    Args:
        project_id (str): DataRobot project ID.
        blueprint_id (str): DataRobot blueprint ID.
        featurelist_id (str, optional): The identifier of the featurelist to use. If not defined,
            the default for this project is used.
        source_project_id (str, optional): Which project created this blueprint_id. If None, it defaults to
            looking in this project. Note that you must have read permissions in this project.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Model training job ID.
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
    Submit a job to the queue to retrain a model on a specific sample size and/or custom featurelist.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        featurelist_id (str, optional): The identifier of the featurelist to use. If not defined, the default
            for this project is used.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Model retraining job ID.
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


class AdvancedTuneModelOperator(BaseDatarobotOperator):
    """
    Advanced tune a model using a set of parameters.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        parameters (List[Tuple[str, str, Any]]): List of tuples containing the task name, parameter name, and value.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Model retraining job ID.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id", "model_id", "parameters"]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        parameters: List[Tuple[str, str, Any]],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.parameters = parameters

    def validate(self) -> None:
        if self.project_id is None:
            raise ValueError("project_id is required.")

        if self.model_id is None:
            raise ValueError("model_id is required.")

        if self.parameters is None:
            raise ValueError("parameters is required.")

    def execute(self, context: Context) -> str:
        model = dr.Model.get(self.project_id, self.model_id)
        tune = model.start_advanced_tuning_session()

        for task_name, parameter_name, value in self.parameters:
            tune.set_parameter(task_name=task_name, parameter_name=parameter_name, value=value)

        job = tune.run()
        return job.id
