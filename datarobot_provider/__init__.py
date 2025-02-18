# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import TypedDict

ConnectionType = TypedDict(
    "ConnectionType",
    {
        "hook-class-name": str,
        "connection-type": str,
    },
)


ProviderInfoType = TypedDict(
    "ProviderInfoType",
    {
        "package-name": str,
        "name": str,
        "description": str,
        "versions": list[str],
        "connection-types": list[ConnectionType],
        "extra-links": list[str],
    },
)


def get_provider_info() -> ProviderInfoType:
    return {
        "package-name": "airflow-provider-datarobot",
        "name": "DataRobot Airflow Provider",
        "description": "DataRobot Airflow provider.",
        "versions": ["0.1.0"],
        "connection-types": [
            {
                "hook-class-name": "datarobot_provider.hooks.datarobot.DataRobotHook",
                "connection-type": "http",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.connections.JDBCDataSourceHook",
                "connection-type": "datarobot.datasource.jdbc",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.BasicCredentialsHook",
                "connection-type": "datarobot.credentials.basic",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.GoogleCloudCredentialsHook",
                "connection-type": "datarobot.credentials.gcp",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.AwsCredentialsHook",
                "connection-type": "datarobot.credentials.aws",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.AzureStorageCredentialsHook",
                "connection-type": "datarobot.credentials.azure",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.OAuthCredentialsHook",
                "connection-type": "datarobot.credentials.oauth",
            },
        ],
        "extra-links": [],
    }
