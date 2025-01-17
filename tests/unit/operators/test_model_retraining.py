# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest
from datarobot import SCORING_TYPE

from datarobot_provider.operators.model_training import RetrainModelOperator


def test_operator_retrain_model(mocker):
    project_id = 'test-project-id'
    model_id = 'test-model-id'
    featurelist_id = 'test-featurelist-id'
    training_row_count = 10000
    sample_pct = 60
    scoring_type = SCORING_TYPE.cross_validation
    job_id = 123

    model_mock = mocker.Mock()
    model_mock.id = model_id
    model_mock.project_id = project_id

    get_model_mock = mocker.patch.object(dr.Model, 'get', return_value=model_mock)

    request_retrain_model_mock = mocker.patch.object(model_mock, 'train', return_value=job_id)

    operator = RetrainModelOperator(
        task_id='train_model',
        project_id=project_id,
        model_id=model_id,
        featurelist_id=featurelist_id,
    )

    result = operator.execute(
        context={
            'params': {
                'sample_pct': sample_pct,
                'scoring_type': scoring_type,
                'training_row_count': training_row_count,
            }
        }
    )

    get_model_mock.assert_called_with(project_id, model_id)
    request_retrain_model_mock.assert_called_with(
        sample_pct=sample_pct,
        featurelist_id=featurelist_id,
        scoring_type=scoring_type,
        training_row_count=training_row_count,
    )
    assert result == job_id


def test_operator_train_model_no_project_id():
    project_id = None
    model_id = 'test-model-id'

    operator = RetrainModelOperator(
        task_id='retrain_model', project_id=project_id, model_id=model_id
    )

    with pytest.raises(ValueError):
        operator.execute(context={'params': {}})


def test_operator_train_model_no_model_id():
    project_id = 'test-project-id'
    model_id = None

    operator = RetrainModelOperator(
        task_id='retrain_model', project_id=project_id, model_id=model_id
    )

    with pytest.raises(ValueError):
        operator.execute(context={'params': {}})
