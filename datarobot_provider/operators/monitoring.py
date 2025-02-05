# Copyright 2023 DataRobot, Inc. and its affiliates.
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

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%s"


class GetServiceStatsOperator(BaseDatarobotOperator):
    """
    Gets service stats measurements from a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Service stats for a Deployment
    :rtype: list[dict]
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]

    def __init__(
        self,
        *,
        deployment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id

    def execute(self, context: Context) -> list[dict]:
        self.log.info(f"Getting service stats for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        service_stats_params = context["params"].get("service_stats", {})
        service_stats = deployment.get_service_stats(**service_stats_params)
        return _serialize_metrics(service_stats)


def _serialize_metrics(service_stats_obj, date_format=DATETIME_FORMAT):
    service_stats_dict = service_stats_obj.__dict__.copy()
    service_stats_dict["period"] = {
        "start": service_stats_obj.period["start"].strftime(date_format),
        "end": service_stats_obj.period["end"].strftime(date_format),
    }
    return service_stats_dict


class GetAccuracyOperator(BaseDatarobotOperator):
    """
    Gets the accuracy of a deployment’s predictions.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: accuracy for a Deployment
    :rtype: list[dict]
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]

    def __init__(
        self,
        *,
        deployment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id

    def execute(self, context: Context) -> list[dict]:
        self.log.info(f"Getting service stats for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(self.deployment_id)
        accuracy_params = context["params"].get("accuracy", {})
        accuracy = deployment.get_accuracy(**accuracy_params)
        return _serialize_metrics(accuracy)


class GetMonitoringSettingsOperator(BaseDatarobotOperator):
    """
    Get monitoring settings for deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]

    def __init__(
        self,
        *,
        deployment_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id

    def execute(self, context: Context) -> dict:
        self.log.info(f"Get Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        drift_tracking_settings = deployment.get_drift_tracking_settings()
        association_id_settings = deployment.get_association_id_settings()
        predictions_data_collection_settings = deployment.get_predictions_data_collection_settings()

        monitoring_settings = {
            "drift_tracking_settings": drift_tracking_settings,
            "association_id_settings": association_id_settings,
            "predictions_data_collection_settings": predictions_data_collection_settings,
        }

        return monitoring_settings


class UpdateMonitoringSettingsOperator(BaseDatarobotOperator):
    """
    Updates monitoring settings for a deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]

    def __init__(
        self,
        *,
        deployment_id: str,
        monitoring_settings: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.monitoring_settings = monitoring_settings

    def execute(self, context: Context) -> None:
        self.log.info(f"Getting Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        current_drift_tracking_settings = deployment.get_drift_tracking_settings()

        target_drift_enabled = context["params"].get(
            "target_drift_enabled", current_drift_tracking_settings["target_drift"]["enabled"]
        )
        feature_drift_enabled = context["params"].get(
            "feature_drift_enabled", current_drift_tracking_settings["feature_drift"]["enabled"]
        )

        if (target_drift_enabled != current_drift_tracking_settings["target_drift"]["enabled"]) or (
            feature_drift_enabled != current_drift_tracking_settings["feature_drift"]["enabled"]
        ):
            self.log.debug(
                f"Trying to update drift settings for deployment_id={self.deployment_id}"
            )
            deployment.update_drift_tracking_settings(
                target_drift_enabled=target_drift_enabled,
                feature_drift_enabled=feature_drift_enabled,
            )
            self.log.info(
                f"Deployment drift settings updated for deployment_id={self.deployment_id}"
            )
        else:
            self.log.info(
                f"No need to update drift settings for deployment_id={self.deployment_id}"
            )

        # Possibly a bug: deployment.get_association_id_settings returns str
        current_association_id_settings: dict = deployment.get_association_id_settings()  # type: ignore[assignment]

        association_id_column = context["params"].get(
            "association_id_column", current_association_id_settings["column_names"]
        )
        required_in_prediction_requests = context["params"].get(
            "required_association_id",
            current_association_id_settings["required_in_prediction_requests"],
        )

        if (association_id_column != current_association_id_settings["column_names"]) or (
            required_in_prediction_requests
            != current_association_id_settings["required_in_prediction_requests"]
        ):
            deployment.update_association_id_settings(
                column_names=association_id_column,
                required_in_prediction_requests=required_in_prediction_requests,
            )
            self.log.info(
                f"Deployment association_id settings updated for deployment_id={self.deployment_id}"
            )
        else:
            self.log.info(
                f"No need to update association_id settings for deployment_id={self.deployment_id}"
            )

        current_predictions_data_collection_settings = (
            deployment.get_predictions_data_collection_settings()
        )
        predictions_data_collection_settings = context["params"].get(
            "predictions_data_collection_enabled",
            current_predictions_data_collection_settings["enabled"],
        )
        if (
            predictions_data_collection_settings
            != current_predictions_data_collection_settings["enabled"]
        ):
            deployment.update_predictions_data_collection_settings(
                enabled=predictions_data_collection_settings,
            )
            self.log.info(
                f"Deployment predictions data collection settings updated for deployment_id={self.deployment_id}"
            )
        else:
            self.log.info(
                f"No need to update predictions data collection settings for deployment_id={self.deployment_id}"
            )
