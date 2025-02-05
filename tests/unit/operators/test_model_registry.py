import datarobot as dr

from datarobot_provider.hooks.datarobot import DataRobotHook
from datarobot_provider.operators.model_registry import CreateRegisteredModelVersionOperator


def test_create_registered_model_version_leaderboard(mocker):
    model_version_params = {
        "model_type": "leaderboard",
        "name": "Test Model",
        "model_id": "123456",
        "registered_model_name": "Test Registry",
    }

    mocker.patch.object(DataRobotHook, "run", return_value=None)
    mock_version = mocker.Mock()
    mock_version.id = "version-123"

    create_mock = mocker.patch.object(
        dr.RegisteredModelVersion, "create_for_leaderboard_item", return_value=mock_version
    )

    operator = CreateRegisteredModelVersionOperator(
        task_id="test_create_model_version",
        model_version_params=model_version_params,
    )

    result = operator.execute(context={})

    create_mock.assert_called_with(
        model_id="123456", name="Test Model", registered_model_name="Test Registry"
    )
    assert result == "version-123"


def test_create_registered_model_version_custom(mocker):
    model_version_params = {
        "model_type": "custom",
        "name": "Custom Model",
        "custom_model_version_id": "987654",
    }

    mocker.patch.object(DataRobotHook, "run", return_value=None)
    mock_version = mocker.Mock()
    mock_version.id = "version-123"

    create_mock = mocker.patch.object(
        dr.RegisteredModelVersion, "create_for_custom_model_version", return_value=mock_version
    )

    operator = CreateRegisteredModelVersionOperator(
        task_id="test_create_model_version_custom",
        model_version_params=model_version_params,
    )

    result = operator.execute(context={})

    create_mock.assert_called_with(custom_model_version_id="987654", name="Custom Model")
    assert result == "version-123"


def test_create_registered_model_version_external(mocker):
    model_version_params = {
        "model_type": "external",
        "name": "External Model",
        "registered_model_id": "123456",
        "target": "classification",
    }

    mocker.patch.object(DataRobotHook, "run", return_value=None)
    mock_version = mocker.Mock()
    mock_version.id = "version-123"

    create_mock = mocker.patch.object(
        dr.RegisteredModelVersion, "create_for_external", return_value=mock_version
    )

    operator = CreateRegisteredModelVersionOperator(
        task_id="test_create_model_version_external",
        model_version_params=model_version_params,
    )

    result = operator.execute(context={})

    create_mock.assert_called_with(
        name="External Model", target="classification", registered_model_id="123456"
    )
    assert result == "version-123"
