# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from enum import Enum
from typing import Any
from typing import Dict

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class ModelType(Enum):
    LEADERBOARD = "leaderboard"
    CUSTOM = "custom"
    EXTERNAL = "external"


class CreateRegisteredModelVersionOperator(BaseDatarobotOperator):
    """
    Dynamically creates a registered model version using one of three methods:
    - Leaderboard Model
    - Custom Model Version
    - External Model

    :param model_version_params: Dictionary with parameters for creating model version.
    """

    template_fields = ["model_version_params"]

    def __init__(
        self,
        *,
        model_version_params: Dict[str, Any],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_version_params = model_version_params
        self.model_creation_methods = {
            ModelType.LEADERBOARD: self.create_for_leaderboard,
            ModelType.CUSTOM: self.create_for_custom,
            ModelType.EXTERNAL: self.create_for_external,
        }

    def validate(self) -> None:
        """Validate required parameters before execution."""
        if not self.model_version_params.get("model_type"):
            raise ValueError("'model_type' must be specified in model_version_params.")

        try:
            model_type_enum = ModelType(self.model_version_params["model_type"])
        except ValueError:
            raise AirflowException(
                f"Invalid model_type: {self.model_version_params['model_type']}"
            ) from None

        if model_type_enum not in self.model_creation_methods:
            raise AirflowException(
                f"Unsupported model_type: {self.model_version_params['model_type']}"
            )

    def execute(self, context: Context) -> str:
        """Executes the operator to create a registered model version."""
        model_type_enum = ModelType(self.model_version_params["model_type"])
        create_method = self.model_creation_methods[model_type_enum]
        extra_params = {k: v for k, v in self.model_version_params.items() if k != "model_type"}
        version = create_method(**extra_params)
        self.log.info(f"Successfully created model version: {version.id}")
        return version.id

    def create_for_leaderboard(self, **kwargs):
        """Creates a registered model version from a leaderboard model."""
        return dr.RegisteredModelVersion.create_for_leaderboard_item(
            model_id=kwargs.pop("model_id"), **kwargs
        )

    def create_for_custom(self, **kwargs):
        """Creates a registered model version for a custom model."""
        return dr.RegisteredModelVersion.create_for_custom_model_version(
            custom_model_version_id=kwargs.pop("custom_model_version_id"), **kwargs
        )

    def create_for_external(self, **kwargs):
        """Creates a registered model version for an external model."""
        return dr.RegisteredModelVersion.create_for_external(
            target=kwargs.pop("target"), name=kwargs.pop("name"), **kwargs
        )
