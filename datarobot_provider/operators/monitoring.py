# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import datarobot as dr
from airflow.utils.context import Context
from datarobot.models.deployment import FeatureDrift
from datarobot.models.deployment import TargetDrift

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


class GetTargetDrift(BaseDatarobotOperator):
    """
    Retrieve target drift information over a certain time period for a DataRobot deployment using DataRobot's API.

    This operator retrieves target drift over a certain time period for an existing deployment by calling
    the deployment's `get_target_drift()` method. It allows optional extra parameters
    to be passed to the DataRobot client call.

    Args:
        deployment_id (str): The ID of the deployment to update.
        start_time (datetime): start of the time period.
        end_time (datetime):  end of the time period.
        extra_params (dict, optional): A dictionary of additional parameters to pass
            to the DataRobot deployment creation API.
        kwargs (dict): Additional keyword arguments passed to the BaseDatarobotOperator.
    """

    template_fields: Sequence[str] = ["deployment_id", "start_time", "end_time", "extra_params"]

    def __init__(
        self,
        *,
        deployment_id: str,
        start_time: datetime,
        end_time: datetime,
        extra_params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.start_time = start_time
        self.end_time = end_time
        self.extra_params = extra_params or {}

    def validate(self) -> None:
        if not self.deployment_id:
            raise ValueError("deployment_id must be provided.")

    def execute(self, context: Context) -> TargetDrift:
        self.log.info("retrieve Target Drift for deployment: %s", self.deployment_id)
        deployment = dr.Deployment(id=self.deployment_id)
        target_drift = deployment.get_target_drift(
            start_time=self.start_time,
            end_time=self.end_time,
            **self.extra_params,
        )
        self.log.info("queried target drift information for deployment: %s", self.deployment_id)
        return target_drift


class GetFeatureDrift(BaseDatarobotOperator):
    """
    Retrieve drift information for deployment's features over a certain time period
    for a DataRobot deployment using DataRobot's API.

    This operator retrieves drift information for deployment's features over a certain time
    period for an existing deployment by calling
    the deployment's `get_feature_drift()` method. It allows optional extra parameters
    to be passed to the DataRobot client call.

    Args:
        deployment_id (str): The ID of the deployment to update.
        start_time (datetime): start of the time period.
        end_time (datetime):  end of the time period.
        extra_params (dict, optional): A dictionary of additional parameters to pass
            to the DataRobot deployment creation API.
        kwargs (dict): Additional keyword arguments passed to the BaseDatarobotOperator.
    """

    template_fields: Sequence[str] = ["deployment_id", "start_time", "end_time", "extra_params"]

    def __init__(
        self,
        *,
        deployment_id: str,
        start_time: datetime,
        end_time: datetime,
        extra_params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.start_time = start_time
        self.end_time = end_time
        self.extra_params = extra_params or {}

    def validate(self) -> None:
        if not self.deployment_id:
            raise ValueError("deployment_id must be provided.")

    def execute(self, context: Context) -> List[FeatureDrift]:
        self.log.info("retrieve Feature Drift for deployment: %s", self.deployment_id)
        deployment = dr.Deployment(id=self.deployment_id)
        feature_drift_data = deployment.get_feature_drift(
            start_time=self.start_time,
            end_time=self.end_time,
            **self.extra_params,
        )
        self.log.info("queried feature drift information for deployment: %s", self.deployment_id)
        return feature_drift_data


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


class UpdateDriftTrackingOperator(BaseDatarobotOperator):
    """
    Update drift tracking settings for a DataRobot deployment using DataRobot's API.

    This operator updates drift tracking settings for an existing deployment by calling
    the deployment's `update_drift_tracking_settings()` method. It allows optional extra parameters
    to be passed to the DataRobot client call.

    Args:
        deployment_id (str): The ID of the deployment to update.
        target_drift_enabled (bool): Boolean flag to enable target drift tracking.
        feature_drift_enabled (bool): Boolean flag to enable feature drift tracking.
        kwargs (dict): Additional keyword arguments passed to the BaseDatarobotOperator.
    """

    template_fields: Sequence[str] = [
        "deployment_id",
        "target_drift_enabled",
        "feature_drift_enabled",
    ]

    def __init__(
        self,
        *,
        deployment_id: str,
        target_drift_enabled: bool = True,
        feature_drift_enabled: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.target_drift_enabled = target_drift_enabled
        self.feature_drift_enabled = feature_drift_enabled

    def validate(self) -> None:
        if not self.deployment_id:
            raise ValueError("deployment_id must be provided.")

    def execute(self, context: Context) -> str:
        self.log.info("Updating drift tracking settings for deployment: %s", self.deployment_id)
        deployment = dr.Deployment(id=self.deployment_id)
        deployment.update_drift_tracking_settings(
            target_drift_enabled=self.target_drift_enabled,
            feature_drift_enabled=self.feature_drift_enabled,
        )
        self.log.info("Drift tracking settings updated for deployment: %s", self.deployment_id)
        return self.deployment_id
