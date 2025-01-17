# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datarobot as dr
import pytest
from datarobot.models.status_check_job import StatusCheckJob

from datarobot_provider.operators.mlops import SubmitActualsFromCatalogOperator


@pytest.fixture
def submit_actuals_from_catalog_settings():
    return {
        'association_id_column': 'id',
        'actual_value_column': 'ACTUAL',
        'timestamp_column': 'test-timestamp_column',
        'was_acted_on_column': 'test-was_acted_on_column',
    }


def test_operator_submit_actuals_from_catalog(mocker, submit_actuals_from_catalog_settings):
    deployment_id = 'test-deployment-id'
    dataset_id = 'test-dataset-id'
    dataset_version_id = 'test-dataset-version-id'
    status_check_job_id = 'test-status-job-id'
    mocker.patch.object(dr.Deployment, 'get', return_value=dr.Deployment(deployment_id))
    submit_actuals_mock = mocker.patch.object(
        dr.Deployment,
        'submit_actuals_from_catalog_async',
        return_value=StatusCheckJob(job_id=status_check_job_id),
    )

    operator = SubmitActualsFromCatalogOperator(
        task_id='submit_actuals_form_catalog',
        deployment_id=deployment_id,
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
    )
    job_status_id = operator.execute(
        context={
            'params': submit_actuals_from_catalog_settings,
        }
    )

    assert job_status_id == status_check_job_id
    submit_actuals_mock.assert_called_with(
        dataset_id=dataset_id,
        dataset_version_id=dataset_version_id,
        association_id_column=submit_actuals_from_catalog_settings['association_id_column'],
        actual_value_column=submit_actuals_from_catalog_settings['actual_value_column'],
        timestamp_column=submit_actuals_from_catalog_settings['timestamp_column'],
        was_acted_on_column=submit_actuals_from_catalog_settings['was_acted_on_column'],
    )


def test_operator_submit_actuals_deployment_is_none(mocker, submit_actuals_from_catalog_settings):
    deployment_id = None
    dataset_id = 'test-dataset-id'
    status_check_job_id = 'test-status-job-id'
    mocker.patch.object(dr.Deployment, 'get', return_value=dr.Deployment(deployment_id))
    mocker.patch.object(
        dr.Deployment,
        'submit_actuals_from_catalog_async',
        return_value=StatusCheckJob(job_id=status_check_job_id),
    )

    with pytest.raises(ValueError):
        operator = SubmitActualsFromCatalogOperator(
            task_id='submit_actuals_form_catalog',
            deployment_id=deployment_id,
            dataset_id=dataset_id,
        )
        operator.execute(
            context={
                'params': submit_actuals_from_catalog_settings,
            }
        )


def test_operator_submit_actuals_dataset_is_none(mocker, submit_actuals_from_catalog_settings):
    deployment_id = 'test-deployment-id'
    dataset_id = None
    status_check_job_id = 'test-status-job-id'
    mocker.patch.object(dr.Deployment, 'get', return_value=dr.Deployment(deployment_id))
    mocker.patch.object(
        dr.Deployment,
        'submit_actuals_from_catalog_async',
        return_value=StatusCheckJob(job_id=status_check_job_id),
    )

    with pytest.raises(ValueError):
        operator = SubmitActualsFromCatalogOperator(
            task_id='submit_actuals_form_catalog',
            deployment_id=deployment_id,
            dataset_id=dataset_id,
        )
        operator.execute(
            context={
                'params': submit_actuals_from_catalog_settings,
            }
        )
