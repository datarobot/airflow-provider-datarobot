# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import os
from collections.abc import Sequence
from typing import Any
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class DownloadDeploymentScoringCodeOperator(BaseDatarobotOperator):
    """
    Downloads scoring code from a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param base_path: base path for storing downloaded model artifact
    :type base_path: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: path to the downloaded jar file
    :rtype: String
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id", "base_path"]

    def __init__(
        self,
        *,
        deployment_id: str,
        base_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.base_path = base_path

    def execute(self, context: Context) -> str:
        self.log.info(f"Getting model scoring code for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        # in case if params provided
        self.base_path = context["params"].get("scoring_code_filepath", self.base_path)
        if self.base_path is None or not os.path.exists(self.base_path):
            raise ValueError("Invalid or missing base path value, make sure that directory exists")

        if not deployment.model:
            raise AirflowException(
                f"Deployment {self.deployment_id} does not have a model associated"
            )

        scoring_code_path = os.path.join(
            self.base_path, f"{deployment.id}-{deployment.model['id']}.jar"
        )

        source_code = context["params"].get("source_code", False)
        include_agent = context["params"].get("include_agent", False)
        include_prediction_explanations = context["params"].get(
            "include_prediction_explanations", False
        )
        include_prediction_intervals = context["params"].get("include_prediction_intervals", False)

        self.log.debug(f"Trying to download scoring code for deployment_id={self.deployment_id}")

        deployment.download_scoring_code(
            filepath=scoring_code_path,
            source_code=source_code,
            include_agent=include_agent,
            include_prediction_explanations=include_prediction_explanations,
            include_prediction_intervals=include_prediction_intervals,
        )

        self.log.info(
            f"Scoring code for deployment_id={self.deployment_id} downloaded to the filepath={scoring_code_path}"
        )

        return scoring_code_path


class DownloadModelScoringCodeOperator(BaseDatarobotOperator):
    """
    Downloads scoring code from a model.

    :param project_id: DataRobot project ID
    :type project_id: str
    :param model_id: DataRobot model ID
    :type model_id: str
    :param base_path: base path for storing downloaded model artifact
    :type base_path: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: path to the downloaded jar file
    :rtype: String
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id", "model_id"]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        base_path: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id
        self.base_path = base_path

    def execute(self, context: Context) -> str:
        # in case if params provided
        self.base_path = context["params"].get("scoring_code_filepath", self.base_path)
        if self.base_path is None or not os.path.exists(self.base_path):
            raise ValueError("Invalid or missing base path value, make sure that directory exists")

        scoring_code_path = os.path.join(self.base_path, f"{self.model_id}.jar")

        source_code = context["params"].get("source_code", False)

        self.log.info(
            f"Trying to download scoring code for project_id={self.project_id} and model_id={self.model_id}"
        )

        model = dr.Model.get(project=self.project_id, model_id=self.model_id)
        model.download_scoring_code(file_name=scoring_code_path, source_code=source_code)

        self.log.info(
            f"Scoring code for project_id={self.project_id} and model_id={self.model_id} "
            f"downloaded to the filepath={scoring_code_path}"
        )

        return scoring_code_path
