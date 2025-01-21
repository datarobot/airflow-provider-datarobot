# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.datarobot import DataRobotHook

if TYPE_CHECKING:
    from datarobot.models.deployment.deployment import BiasAndFairnessSettings


class GetBiasAndFairnessSettingsOperator(BaseOperator):
    """
    Get Bias And Fairness settings for deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> Optional["BiasAndFairnessSettings"]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Get Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        bias_and_fairness_settings = deployment.get_bias_and_fairness_settings()

        return bias_and_fairness_settings


class UpdateBiasAndFairnessSettingsOperator(BaseOperator):
    """
    Update Bias And Fairness settings for deployment.

    :param deployment_id: DataRobot deployment ID
    :type deployment_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["deployment_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        deployment_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> None:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.info(f"Getting Deployment for deployment_id={self.deployment_id}")
        deployment = dr.Deployment.get(deployment_id=self.deployment_id)

        current_bias_and_fairness_settings = deployment.get_bias_and_fairness_settings() or {
            "protected_features": [],
            "fairness_metric_set": "",
            "fairness_threshold": 0.0,
            "preferable_target_value": False,
        }

        protected_features = context["params"].get(
            "protected_features", current_bias_and_fairness_settings["protected_features"]
        )

        fairness_metric_set = context["params"].get(
            "fairness_metric_set",
            # Backward compatibility with parameter name fairness_metrics_set
            context["params"].get(
                "fairness_metrics_set", current_bias_and_fairness_settings["fairness_metric_set"]
            ),
        )

        fairness_threshold = context["params"].get(
            "fairness_threshold", current_bias_and_fairness_settings["fairness_threshold"]
        )

        preferable_target_value = context["params"].get(
            "preferable_target_value", current_bias_and_fairness_settings["preferable_target_value"]
        )

        if (
            (protected_features != current_bias_and_fairness_settings["protected_features"])
            or (fairness_metric_set != current_bias_and_fairness_settings["fairness_metric_set"])
            or (fairness_threshold != current_bias_and_fairness_settings["fairness_threshold"])
            or (
                preferable_target_value
                != current_bias_and_fairness_settings["preferable_target_value"]
            )
        ):
            self.log.debug(
                f"Trying to update bias and fairness settings for deployment_id={self.deployment_id}"
            )
            deployment.update_bias_and_fairness_settings(
                protected_features=protected_features,
                fairness_metric_set=fairness_metric_set,
                fairness_threshold=fairness_threshold,
                preferable_target_value=preferable_target_value,
            )
            self.log.info(
                f"Deployment bias and fairness settings updated for deployment_id={self.deployment_id}"
            )
        else:
            self.log.info(
                f"No need to update bias and fairness settings for deployment_id={self.deployment_id}"
            )
