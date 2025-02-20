# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import tempfile
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import black
import pytest
import yaml
from numpydoc.docscrape import NumpyDocString
from numpydoc.docscrape import Parameter

from datarobot_provider.autogen.generate_operator import GenerateOperators
from datarobot_provider.autogen.generate_operator import main


@pytest.fixture
def generator():
    whitelist = {"datarobot.models.modeljob": {"ModelJob": ["get"]}}
    return GenerateOperators(whitelist)


@pytest.fixture
def docstring_fixture():
    docstring = NumpyDocString("")
    docstring["Summary"] = ["Fetches one ModelJob."]
    docstring["Parameters"] = [
        Parameter("project_id", "str", ["The identifier of the project the model belongs to"]),
        Parameter("model_job_id", "str", ["The identifier of the model_job"]),
    ]
    docstring["Returns"] = [Parameter("model_job", "ModelJob", [""])]
    return docstring


def test_generate_operator_from_test_whitelist(tests_fixtures_dir):
    with open(f"{tests_fixtures_dir}/whitelist_test.yaml", "r") as f:
        whitelist = yaml.safe_load(f)
    with open(f"{tests_fixtures_dir}/whitelist_test_output.txt") as f:
        example_output = f.read()

    generator = GenerateOperators(whitelist)
    results = generator.generate()
    assert results["operators.modeljob.ModelJobGetOperator"] == example_output


@patch("datarobot_provider.autogen.generate_operator.black.format_file_contents")
def test_format_code_block_with_black(mock_format_file_contents, generator):
    mock_format_file_contents.return_value = "formatted_code"
    code_block = "some_code"
    result = generator.format_code_block_with_black(code_block)
    mock_format_file_contents.assert_called_once_with(code_block, fast=False, mode=black.FileMode())
    assert result == "formatted_code"


@patch("datarobot_provider.autogen.generate_operator.importlib.import_module")
@patch.object(GenerateOperators, "format_code_block_with_black", return_value="formatted_code")
@patch.object(GenerateOperators, "construct_operator_docstring", return_value="docstring")
@patch.object(GenerateOperators, "construct_operator_attibutes", return_value="attributes")
@patch.object(GenerateOperators, "construct_operator_init", return_value="init")
@patch.object(GenerateOperators, "construct_operator_execute", return_value="execute")
def test_generate_operator_for_method(
    mock_execute,
    mock_init,
    mock_attributes,
    mock_docstring,
    mock_format_code_block_with_black,
    mock_import_module,
    generator,
):
    mock_module = MagicMock()
    mock_import_module.return_value = mock_module
    mock_method = MagicMock()
    mock_method.__doc__ = "method docstring"
    mock_module.ModelJob.get = mock_method

    result = generator.generate_operator_for_method(
        "datarobot.models.modeljob", "ModelJob", "get", "method docstring"
    )

    assert result == "formatted_code"
    mock_format_code_block_with_black.assert_called_once()
    mock_docstring.assert_called_once()
    mock_attributes.assert_called_once()
    mock_init.assert_called_once()
    mock_execute.assert_called_once()


def test_construct_operator_docstring(generator, docstring_fixture):
    result = generator.construct_operator_docstring(docstring_fixture)
    expected = (
        '\t"""\tFetches one ModelJob.\n\n'
        "\t:param project_id: The identifier of the project the model belongs to\n"
        "\t:type project_id: str\n"
        "\t:param model_job_id: The identifier of the model_job\n"
        "\t:type model_job_id: str\n"
        "\t:return: model_job\n"
        "\t:rtype ModelJob\n"
        '\t"""\n'
    )
    assert result == expected


def test_construct_operator_attibutes(generator, docstring_fixture):
    result = generator.construct_operator_attibutes(docstring_fixture)
    expected = (
        "\ttemplate_fields: Sequence[str] = ['project_id', 'model_job_id']\n"
        "\ttemplate_fields_renderers: dict[str, str] = {}\n"
        "\ttemplate_ext: Sequence[str] = ()\n"
        '\tui_color = "#f4a460"\n'
    )
    assert result == expected


def test_construct_operator_init(generator, docstring_fixture):
    result = generator.construct_operator_init(docstring_fixture)
    expected = (
        "\tdef __init__(self,*,project_id: Optional[str] = None,model_job_id: Optional[str] = None,datarobot_conn_id: str = DATAROBOT_CONN_ID,\n"
        "**kwargs: Any) -> None:\n"
        "\t\tsuper().__init__(**kwargs)\n"
        "\t\tself.project_id = project_id\n"
        "\t\tself.model_job_id = model_job_id\n"
        "\t\tself.datarobot_conn_id = datarobot_conn_id\n"
    )
    assert result == expected


def test_construct_operator_execute(generator, docstring_fixture):
    result = generator.construct_operator_execute(
        docstring_fixture, "datarobot.models.modeljob", "ModelJob", "get"
    )
    expected = (
        "\tdef execute(self, context: Context) -> str:\n"
        "\t\t# Initialize DataRobot client\n"
        "\t\tDataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()\n\n"
        "\t\tif self.project_id is None:\n"
        '\t\t\traise ValueError("project_id is required for ModelJob.")\n'
        "\t\tif self.model_job_id is None:\n"
        '\t\t\traise ValueError("model_job_id is required for ModelJob.")\n\n'
        "\t\tresult = ModelJob.get(\n\t\tself.project_id, self.model_job_id, \n\t)\n"
        "\t\treturn result.id\n"
    )
    assert result == expected


@patch.object(GenerateOperators, "generate")
def test_main(mock_generate, tests_fixtures_dir):
    mock_generate.return_value = {"operators.modeljob.ModelJobGetOperator": "code"}

    with tempfile.TemporaryDirectory() as tempdir:
        whitelist_path = f"{tests_fixtures_dir}/whitelist_test.yaml"
        main(Path(whitelist_path), Path(tempdir))
        with open(f"{tempdir}/modeljob/model_job_get_operator.py") as f:
            result = f.read()
            assert result == "code"
        with open(f"{tempdir}/modeljob/__init__.py") as f:
            result = f.read()
            assert (
                result
                == "from .model_job_get_operator import ModelJobGetOperator as ModelJobGetOperator\n"
            )
