# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr

from datarobot_provider.sensors.model_training import ModelTrainingJobSensor


def test_model_training_job_sensor__success(mocker):
    trained_model_id = 'trained-model-id'

    job_mock = mocker.Mock()
    job_mock.status = 'COMPLETED'

    model_mock = mocker.Mock()
    model_mock.id = trained_model_id

    mocker.patch.object(dr.Job, 'get', return_value=job_mock)
    mocker.patch.object(dr.ModelJob, 'get_model', return_value=model_mock)

    operator = ModelTrainingJobSensor(
        task_id='check_model_training_job_finished',
        project_id='project-id',
        job_id='job-id',
    )
    result = operator.poke(context={})

    assert result.is_done is True
    assert result.xcom_value == trained_model_id
