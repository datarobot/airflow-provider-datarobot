# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest
from airflow.exceptions import AirflowNotFoundException

from datarobot_provider.operators.connections import GetOrCreateDataStoreOperator


def test_operator_get_or_create_dataset(mock_airflow_connection_datarobot_jdbc):
    test_params = {
        "datarobot_connection_name": "datarobot_jdbc_default",
    }

    operator = GetOrCreateDataStoreOperator(
        task_id='get_datastore_id', connection_param_name='datarobot_connection_name'
    )

    dataset_id = operator.execute(
        context={
            "params": test_params,
        }
    )

    assert dataset_id == "test-datastore-id"


def test_operator_get_or_create_dataset_not_found(mock_airflow_connection_datarobot_jdbc):
    test_params = {
        "datarobot_connection_name": "datarobot_jdbc_not_found",
    }

    with pytest.raises(AirflowNotFoundException):
        operator = GetOrCreateDataStoreOperator(
            task_id='get_datastore_id', connection_param_name='datarobot_connection_name'
        )
        operator.execute(
            context={
                "params": {
                    "params": test_params,
                },
            }
        )
