# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any

from airflow import __version__ as AIRFLOW_VERSION
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from datarobot.client import Client
from datarobot.rest import RESTClientObject

from datarobot_provider import get_provider_info


class DataRobotHook(BaseHook):
    """
    A hook that interacts with DataRobot via its public Python API library.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    """

    conn_name_attr = "datarobot_conn_id"
    default_conn_name = "datarobot_default"
    conn_type = "http"
    hook_name = "DataRobot"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField
        from wtforms import StringField

        return {
            "extra__http__endpoint": StringField(
                lazy_gettext("DataRobot endpoint URL"),
                widget=BS3TextFieldWidget(),
                default="https://app.datarobot.com/api/v2",
            ),
            "extra__http__api_key": PasswordField(
                lazy_gettext("API Key"), widget=BS3PasswordFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
            "placeholders": {
                "extra__http__endpoint": "https://app.datarobot.com/api/v2",
                "extra__http__api_key": "your-api-key",
            },
        }

    def __init__(
        self,
        datarobot_conn_id: str = default_conn_name,
    ) -> None:
        super().__init__()
        self.datarobot_conn_id = datarobot_conn_id

    def get_conn(self) -> RESTClientObject:
        """Initializes a DataRobot client instance."""
        conn = self.get_connection(self.datarobot_conn_id)
        endpoint = conn.extra_dejson.get("extra__http__endpoint", "")
        if not endpoint:
            raise AirflowException("Endpoint is not defined")
        api_key = conn.extra_dejson.get("extra__http__api_key", "")
        if not api_key:
            raise AirflowException("API key is not defined")

        # Creating version-specific user agent suffix for collecting usage statistics and troubleshoot purposes:
        provider_package_name = get_provider_info().get("package-name")
        provider_versions = "".join(get_provider_info().get("versions"))
        user_agent_suffix = "{}-{}-airflow-{}".format(
            provider_package_name, provider_versions, AIRFLOW_VERSION
        )
        self.log.info("Initialize DataRobot Client, user_agent_suffix:{}".format(user_agent_suffix))
        return Client(token=api_key, endpoint=endpoint, trace_context=user_agent_suffix)

    def run(self) -> Any:
        # Initialize DataRobot client
        return self.get_conn()

    def test_connection(self):
        """Test HTTP Connection"""
        try:
            self.run()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
