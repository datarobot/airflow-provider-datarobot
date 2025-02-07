from unittest.mock import ANY

import datarobot as dr
import pytest
from airflow.exceptions import AirflowException

from datarobot_provider.operators.base_datarobot_operator import XCOM_DEFAULT_USE_CASE_ID
from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator


@pytest.mark.parametrize(
    "use_case_id, default_use_case_id, expected_use_case_id, is_pulled",
    [
        ("explicit-id", "xcom-id", "explicit-id", False),
        ("", "xcom-id", "xcom-id", True),
        (None, "xcom-id", "xcom-id", True),
        ("", None, None, True),
    ],
)
def test_base_use_case_entity_get_use_case_id(
    xcom_context, use_case_id, default_use_case_id, expected_use_case_id, is_pulled
):
    xcom_context["ti"].xcom_pull.return_value = default_use_case_id
    operator = BaseUseCaseEntityOperator(task_id="test-task-id", use_case_id=use_case_id)
    actual_use_case_id = operator.get_use_case_id(xcom_context)

    assert actual_use_case_id == expected_use_case_id
    if is_pulled:
        xcom_context["ti"].xcom_pull.assert_called_once_with(
            task_ids=None,
            dag_id=None,
            include_prior_dates=None,
            session=ANY,
            key=XCOM_DEFAULT_USE_CASE_ID,
        )

    else:
        assert not xcom_context["ti"].xcom_pull.called


def test_base_use_case_entity_get_use_case_id_required(xcom_context):
    operator = BaseUseCaseEntityOperator(task_id="test-task-id", use_case_id=None)
    with pytest.raises(AirflowException, match="requires a Use Case"):
        operator.get_use_case_id(xcom_context, required=True)


def test_base_use_case_entity_get_use_case(mocker):
    patched_use_case_get = mocker.patch.object(dr.UseCase, "get")
    operator = BaseUseCaseEntityOperator(task_id="test-task-id", use_case_id="test-id")

    use_case = operator.get_use_case({})

    assert use_case == patched_use_case_get.return_value
    patched_use_case_get.assert_called_once_with("test-id")


def test_base_use_case_entity_add_into_use_case(mocker):
    patched_get_use_case = mocker.patch.object(BaseUseCaseEntityOperator, "get_use_case")
    entity = mocker.Mock()
    context = {}

    operator = BaseUseCaseEntityOperator(task_id="test-task-id", use_case_id="test-id")

    operator.add_into_use_case(entity, context=context)

    patched_get_use_case.assert_called_once_with(context)
    patched_get_use_case.return_value.add.assert_called_once_with(entity)


def test_base_use_case_entity_add_into_use_case_none(mocker):
    patched_get_use_case = mocker.patch.object(
        BaseUseCaseEntityOperator, "get_use_case", return_value=None
    )
    context = {}

    operator = BaseUseCaseEntityOperator(task_id="test-task-id", use_case_id="test-id")

    operator.add_into_use_case(mocker.Mock(), context=context)

    patched_get_use_case.assert_called_once_with(context)
