# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
from datarobot_provider.operators.connections import GetDataStoreOperator


def test_operator_get_data_store(mocker):
    mocker.patch.object(
        dr.DataStore,
        "list",
        return_value=[
            dr.DataStore(data_store_id="0", canonical_name="the connection"),
            dr.DataStore(data_store_id="1", canonical_name="The connection"),
            dr.DataStore(data_store_id="2", canonical_name="The connection."),
        ],
    )

    operator = GetDataStoreOperator(task_id="test", data_connection="The connection")
    data_store_id = operator.execute({})

    assert data_store_id == "1"
