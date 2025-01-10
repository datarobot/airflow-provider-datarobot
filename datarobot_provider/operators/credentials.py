# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict
from typing import Iterable

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from datarobot import Credential

from datarobot_provider.hooks.credentials import CredentialsBaseHook
from datarobot_provider.hooks.datarobot import DataRobotHook


class GetOrCreateCredentialOperator(BaseOperator):
    """
    Fetching credentials by Credential name and return Credentials ID.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot Credentials ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = []
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        credentials_param_name: str = "datarobot_credentials_name",
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        self.credentials_param_name = credentials_param_name
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()
        credential_name = context["params"][self.credentials_param_name]
        # Trying to find a credential associated with provided credential name:
        for credential in Credential.list():
            if (
                credential.name == credential_name
                and credential.description != CredentialsBaseHook.default_credential_description
            ):
                self.log.info(
                    f'Found Credentials :{credential.name} , id={credential.credential_id} '
                    f'for param {self.credentials_param_name}'
                )
                return credential.credential_id
        else:
            # Trying to find an Airflow preconfigured credentials for provided credential name
            # to replicate credentials on DataRobot side:
            self.log.info(
                f'Credentials with name {credential_name} not found in DataRobot, trying to find '
                'Airflow connection with the same name'
            )
            hook = CredentialsBaseHook.get_hook(conn_id=credential_name)
            if hook.conn_type == 'datarobot.datasource.jdbc':
                credentials, _, _ = hook.run()
            else:
                credentials, _ = hook.run()
            return credentials.credential_id
