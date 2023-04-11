# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator


def test_operator_upload_dataset(mocker):
    dataset_mock = mocker.Mock()
    dataset_mock.id = "dataset-id"
    upload_dataset_mock = mocker.patch.object(
        dr.Dataset, "create_from_file", return_value=dataset_mock
    )

    operator = UploadDatasetOperator(task_id='upload_dataset')
    dataset_id = operator.execute(
        context={
            "params": {
                "dataset_file_path": "/path/to/local/file",
            },
        }
    )

    assert dataset_id == "dataset-id"
    upload_dataset_mock.assert_called_with("/path/to/local/file")
