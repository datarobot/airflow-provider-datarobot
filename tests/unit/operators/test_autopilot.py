# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
from datarobot.enums import CV_METHOD
from datarobot.enums import VALIDATION_TYPE

from datarobot_provider.operators.autopilot import StartAutopilotOperator


def test_operator_start_autopilot(mocker):
    project_mock = mocker.Mock(target=None)
    mocker.patch.object(dr.Project, 'get', return_value=project_mock)

    operator = StartAutopilotOperator(task_id='train_models', project_id='project-id')
    autopilot_settings = {'target': 'readmitted'}
    operator.execute(context={'params': {'autopilot_settings': autopilot_settings}})

    project_mock.analyze_and_model.assert_called_with(**autopilot_settings)


def test_operator_start_autopilot_timeseries(mocker):
    project_mock = mocker.Mock(target=None)
    mocker.patch.object(dr.Project, 'get', return_value=project_mock)

    operator = StartAutopilotOperator(task_id='train_models', project_id='project-id')
    autopilot_settings = {'target': 'readmitted'}
    datetime_partitioning_settings = {
        'use_time_series': True,
        'datetime_partition_column': 'datetime',
        'multiseries_id_columns': ['location'],
    }
    operator.execute(
        context={
            'params': {
                'autopilot_settings': autopilot_settings,
                'datetime_partitioning_settings': datetime_partitioning_settings,
            }
        }
    )
    project_mock.set_datetime_partitioning.assert_called_with(**datetime_partitioning_settings)
    project_mock.set_partitioning_method.assert_not_called()
    project_mock.set_options.assert_not_called()
    project_mock.set_datetime_partitioning.assert_called_with(**datetime_partitioning_settings)
    project_mock.analyze_and_model.assert_called_with(**autopilot_settings)


def test_operator_start_autopilot_partitioning_settings(mocker):
    project_mock = mocker.Mock(target=None)
    mocker.patch.object(dr.Project, 'get', return_value=project_mock)

    operator = StartAutopilotOperator(task_id='train_models', project_id='project-id')
    autopilot_settings = {'target': 'readmitted'}
    partitioning_settings = {
        'cv_method': CV_METHOD.RANDOM,
        'validation_type': VALIDATION_TYPE.TVH,
        'validation_pct': 20,
        'holdout_pct': 15,
    }
    operator.execute(
        context={
            'params': {
                'autopilot_settings': autopilot_settings,
                'partitioning_settings': partitioning_settings,
            }
        }
    )
    project_mock.set_datetime_partitioning.assert_not_called()
    project_mock.set_partitioning_method.assert_called_with(**partitioning_settings)
    project_mock.set_options.assert_not_called()
    project_mock.analyze_and_model.assert_called_with(**autopilot_settings)


def test_operator_start_autopilot_advanced_options(mocker):
    project_mock = mocker.Mock(target=None)
    mocker.patch.object(dr.Project, 'get', return_value=project_mock)

    operator = StartAutopilotOperator(task_id='train_models', project_id='project-id')
    autopilot_settings = {'target': 'readmitted'}
    advanced_options = {
        'smart_downsampled': True,
        'only_include_monotonic_blueprints': True,
        'blend_best_models': True,
        'scoring_code_only': True,
    }
    operator.execute(
        context={
            'params': {
                'autopilot_settings': autopilot_settings,
                'advanced_options': advanced_options,
            }
        }
    )
    project_mock.set_datetime_partitioning.assert_not_called()
    project_mock.set_partitioning_method.assert_not_called()
    project_mock.set_options.assert_called_with(**advanced_options)
    project_mock.analyze_and_model.assert_called_with(**autopilot_settings)
