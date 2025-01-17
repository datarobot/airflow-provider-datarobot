# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
"""
Config example for this dag:
{
    "datarobot_jdbc_connection": "datarobot_jdbc_demo",
    "dataset_name": "integration_example_demo",
    "table_schema": "integration_example_demo",
    "table_name": "actuals_demo",
}
"""

from datetime import datetime

from airflow.decorators import dag

from datarobot_provider.operators.ai_catalog import CreateDatasetVersionOperator
from datarobot_provider.operators.ai_catalog import CreateOrUpdateDataSourceOperator
from datarobot_provider.operators.connections import GetOrCreateDataStoreOperator
from datarobot_provider.operators.credentials import GetOrCreateCredentialOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
    # Default json config example:
    params={
        "datarobot_jdbc_connection": "datarobot_jdbc_demo",
        "dataset_name": "integration_example_demo",
        "table_schema": "integration_example_demo",
        "table_name": "actuals_demo",
    },
)
def datarobot_dataset_new_version(dataset_id=None):
    get_credentials_op = GetOrCreateCredentialOperator(
        task_id="get_jdbc_credentials",
        credentials_param_name="datarobot_jdbc_connection",
    )

    get_datastore_op = GetOrCreateDataStoreOperator(
        task_id="get_datastore",
        connection_param_name="datarobot_jdbc_connection",
    )

    get_datasource_op = CreateOrUpdateDataSourceOperator(
        task_id="get_datasource",
        data_store_id=get_datastore_op.output,
    )

    create_dataset_version_op = CreateDatasetVersionOperator(
        task_id="create_new_version_of_dataset",
        dataset_id=dataset_id,
        datasource_id=get_datasource_op.output,
        credential_id=get_credentials_op.output,
    )

    get_datastore_op >> get_datasource_op >> get_credentials_op >> create_dataset_version_op


datarobot_dataset_new_version_dag = datarobot_dataset_new_version()

if __name__ == "__main__":
    datarobot_dataset_new_version_dag.test()
