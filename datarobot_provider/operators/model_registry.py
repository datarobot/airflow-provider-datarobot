from enum import Enum
from typing import Any
from typing import Dict
from typing import Sequence

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook


class ModelType(Enum):
    LEADERBOARD = "leaderboard"
    CUSTOM = "custom"
    EXTERNAL = "external"


class CreateRegisteredModelVersionOperator(BaseOperator):
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
        model_name = self.model_version_params.get("name")

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

        version = create_method(model_name)
        self.log.info(f"Successfully created model version: {version.id}")
        return version.id

    def create_for_leaderboard(self, model_name):
        """Creates a registered model version from a leaderboard model."""
        return dr.RegisteredModelVersion.create_for_leaderboard_item(
            model_id=self.model_version_params["model_id"],
            name=model_name,
            registered_model_name=self.model_version_params["registered_model_name"],
        )

    def create_for_custom(self, model_name):
        """Creates a registered model version for a custom model."""
        return dr.RegisteredModelVersion.create_for_custom_model_version(
            custom_model_version_id=self.model_version_params["custom_model_version_id"],
            name=model_name,
        )

    def create_for_external(self, model_name):
        """Creates a registered model version for an external model."""
        return dr.RegisteredModelVersion.create_for_external(
            name=model_name,
            target=self.model_version_params["target"],
            registered_model_id=self.model_version_params["registered_model_id"],
        )
