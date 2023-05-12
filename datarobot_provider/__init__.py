# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
def get_provider_info():
    return {
        "package-name": "airflow-provider-datarobot",
        "name": "DataRobot Airflow Provider",
        "description": "DataRobot Airflow provider.",
        "versions": ["0.0.5"],
        "connection-types": [
            {
                "hook-class-name": "datarobot_provider.hooks.datarobot.DataRobotHook",
                "connection-type": "http",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.connections.JDBCDataSourceHook",
                "connection-type": "datarobot_jdbc_datasource",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.BasicCredentialsHook",
                "connection-type": "datarobotcredentialsbasic",
            },
            {
                "hook-class-name": "datarobot_provider.hooks.credentials.GoogleCloudCredentialsHook",
                "connection-type": "datarobotcredentialsgcp",
            },
        ],
        "extra-links": [],
    }
