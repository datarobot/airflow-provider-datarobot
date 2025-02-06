from enum import Enum
from typing import Any
from typing import Dict
from typing import Sequence

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook
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
    :param datarobot_conn_id: Airflow connection ID for DataRobot.
    """

    template_fields: Sequence[str] = ["model_version_params"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        model_version_params: Dict[str, Any],
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_version_params = model_version_params
        self.datarobot_conn_id = datarobot_conn_id

    def execute(self, context: Context) -> str:
        """Executes the operator to create a registered model version."""
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()
        model_type = self.model_version_params.get("model_type")

        if not model_type:
            raise ValueError("'model_type' must be specified in model_version_params.")

        try:
            model_type_enum = ModelType(model_type)
        except ValueError:
            raise AirflowException(f"Invalid model_type: {model_type}") from None

        model_creation_methods = {
            ModelType.LEADERBOARD: self.create_for_leaderboard,
            ModelType.CUSTOM: self.create_for_custom,
            ModelType.EXTERNAL: self.create_for_external,
        }

        create_method = model_creation_methods.get(model_type_enum)
        if not create_method:
            raise AirflowException(f"Unsupported model_type: {model_type}")

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
