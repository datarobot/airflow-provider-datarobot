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
        "versions": ["0.4.1"],
        "connection-types": [
            {
                "hook-class-name": "datarobot_provider.hooks.datarobot.DataRobotHook",
                "connection-type": "http",
            }
        ],
        "extra-links": [],
    }
