# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Optional

from airflow.utils.context import Context
from datarobot import Credential

from datarobot_provider.hooks.credentials import CredentialsBaseHook
from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class GetOrCreateCredentialOperator(BaseDatarobotOperator):
    """
    Fetching credentials by Credential name and return Credentials ID.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot Credentials ID
    :rtype: str
    """

    def __init__(
        self,
        *,
        credentials_param_name: str = "datarobot_credentials_name",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.credentials_param_name = credentials_param_name

    def execute(self, context: Context) -> Optional[str]:
        credential_name = context["params"][self.credentials_param_name]
        # Trying to find a credential associated with provided credential name:
        for credential in Credential.list():
            if (
                credential.name == credential_name
                and credential.description != CredentialsBaseHook.default_credential_description
            ):
                self.log.info(
                    f"Found Credentials :{credential.name} , id={credential.credential_id} "
                    f"for param {self.credentials_param_name}"
                )
                return credential.credential_id
        else:
            # Trying to find an Airflow preconfigured credentials for provided credential name
            # to replicate credentials on DataRobot side:
            self.log.info(
                f"Credentials with name {credential_name} not found in DataRobot, trying to find "
                "Airflow connection with the same name"
            )
            hook = CredentialsBaseHook.get_hook(conn_id=credential_name)
            if hook.conn_type == "datarobot.datasource.jdbc":  # type: ignore[attr-defined]
                credentials, _, _ = hook.run()  # type: ignore[attr-defined]
            else:
                credentials, _ = hook.run()  # type: ignore[attr-defined]
            return credentials.credential_id
