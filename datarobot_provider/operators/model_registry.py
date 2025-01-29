from typing import Any, Dict, Optional, Sequence
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.context import Context
import datarobot as dr


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
            model_version_params: Dict[str,  Any],
            datarobot_conn_id: str = "datarobot_default",
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_version_params = model_version_params
        self.datarobot_conn_id = datarobot_conn_id

    def execute(self, context: Context) -> str:
        """Executes the operator to create a registered model version."""
        model_type = self.model_version_params.get("model_type")
        model_name = self.model_version_params.get("name")

        if not model_type:
            raise ValueError("'model_type' must be specified in model_version_params.")

        if model_type == "leaderboard":
            model_id = self.model_version_params["model_id"]
            registered_model_name = self.model_version_params["registered_model_name"]

            version = dr.RegisteredModelVersion.create_for_leaderboard_item(
                model_id=model_id, name=model_name, registered_model_name=registered_model_name
            )

        elif model_type == "custom":
            custom_model_version_id = self.model_version_params["custom_model_version_id"]

            version = dr.RegisteredModelVersion.create_for_custom_model_version(
                custom_model_version_id=custom_model_version_id, name=model_name
            )

        elif model_type == "external":
            registered_model_id = self.model_version_params["registered_model_id"]
            target = self.model_version_params["target"]

            version = dr.RegisteredModelVersion.create_for_external(
                name=model_name, target=target, registered_model_id=registered_model_id
            )
        else:
            raise AirflowException(f"Invalid model_type: {model_type}")

        self.log.info(f"Registered Model Version Created: {version.id}")
        return version.id
