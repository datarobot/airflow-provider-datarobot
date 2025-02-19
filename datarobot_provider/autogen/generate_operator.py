# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import argparse
import importlib
from collections import defaultdict
from pathlib import Path

import black
import yaml
from numpydoc.docscrape import NumpyDocString


class GenerateOperators:
    def __init__(self, whitelist):
        self.whitelist = whitelist

    def generate(self):
        """Generate operator python code for each class/method in the whitelist."""
        generated_code = {}
        for module_key in self.whitelist.keys():
            module = importlib.import_module(module_key)
            for module_obj_key, methods in self.whitelist[module_key].items():
                module_obj = getattr(module, module_obj_key)
                for method in methods:
                    operator_name = f"{module_obj_key}{method.title()}Operator"
                    method_docstring = getattr(module_obj, method).__doc__
                    op_python_code = self.generate_operator_for_method(
                        module_key, module_obj_key, method, method_docstring
                    )
                    generated_code[f"operators.{module_obj_key.lower()}.{operator_name}"] = (
                        op_python_code
                    )
        return generated_code

    @classmethod
    def generate_operator_for_method(
        cls, module_key: str, module_obj_key: str, method: str, method_docstring: NumpyDocString
    ) -> str:
        """Generate the operator code for a given method in a module.

        Parameters
        ----------
        module_key : str
            The module name.
        module_obj_key : str
            The object name within the module.
        method : str
            The method name.
        method_docstring : NumpyDocString
            The docstring for the method.

        Returns
        -------
        The generated operator code.
        """
        print(f"Generating operator for {module_obj_key} {method}")
        doc = NumpyDocString(str(method_docstring))
        operator_name = f"{module_obj_key}{method.title()}Operator"
        op_python_code = (
            f"class {operator_name}(BaseOperator):\n"
            + cls.construct_operator_docstring(doc)
            + "\n\n"
            + cls.construct_operator_attibutes(doc)
            + "\n\n"
            + cls.construct_operator_init(doc)
            + "\n\n"
            + cls.construct_operator_execute(doc, module_key, module_obj_key, method)
            + "\n"
        )
        op_python_code = (
            cls.construct_imports(op_python_code, module_key, module_obj_key)
            + "\n\n"
            + op_python_code
        )
        op_python_code = cls.format_code_block_with_black(op_python_code)
        return op_python_code

    @staticmethod
    def construct_imports(code: str, module_key: str, module_obj_key: str) -> str:
        """Construct the imports code for the operator.

        Parameters
        ----------
        module_key : str
            The module name.
        module_obj_key : str
            The object name within the module.

        Returns
        -------
        The constructed imports code.
        """
        imports = [
            ("Sequence", "from collections.abc import Sequence"),
            ("Any", "from typing import Any"),
            ("Optional", "from typing import Optional"),
            ("",),
            ("AirflowException", "from airflow.exceptions import AirflowException"),
            ("BaseOperator", "from airflow.models import BaseOperator"),
            ("Context", "from airflow.utils.context import Context"),
            (f"from {module_key} import {module_obj_key}",),
            ("",),
            ("DATAROBOT_CONN_ID", "from datarobot_provider.constants import DATAROBOT_CONN_ID"),
            ("DataRobotHook", "from datarobot_provider.hooks.datarobot import DataRobotHook"),
        ]
        actual_imports = []
        for imprt in imports:
            # tuples with length one are required imports
            if len(imprt) == 1:
                actual_imports.append(imprt[0])
            # others are optional imports
            else:
                if imprt[0] in code:
                    actual_imports.append(imprt[1])

        return "\n".join(actual_imports)

    @staticmethod
    def format_code_block_with_black(code_block: str) -> str:
        """Reformat a string of python code using black."""
        return str(black.format_file_contents(code_block, fast=False, mode=black.FileMode()))

    @staticmethod
    def construct_operator_docstring(docstring: NumpyDocString) -> str:
        """Construct the operator docstring based on the provided NumpyDocString.

        Parameters
        ----------
        docstring : NumpyDocString
            The NumpyDocString object containing the documentation.

        Returns
        -------
        str
            The constructed operator docstring.
        """
        op_docstring = f'\t"""\t{" ".join(docstring["Summary"])}\n\n'
        for param in docstring["Parameters"]:
            op_docstring += f"\t:param {param.name}: {' '.join(param.desc)}\n"
            op_docstring += f"\t:type {param.name}: {param.type}\n"
        for returns in docstring["Returns"]:
            op_docstring += f"\t:return: {returns.name}\n"
            op_docstring += f"\t:rtype {returns.type}\n"
        op_docstring += '\t"""\n'
        return op_docstring

    @staticmethod
    def construct_operator_attibutes(docstring: NumpyDocString) -> str:
        """Construct the operator attributes code based on the provided NumpyDocString.

        Parameters
        ----------
        docstring : NumpyDocString
            The NumpyDocString object containing the documentation.

        Returns
        -------
        str
            The constructed operator attributes code.
        """
        param_names = [param.name for param in docstring["Parameters"]]
        param_types = [param.type for param in docstring["Parameters"]]
        op_attributes = f"\ttemplate_fields: Sequence[str] = {param_names}\n"
        op_attributes += (
            f"\ttemplate_fields_renderers: dict[{', '.join(map(str, param_types))}] = {{}}\n"
        )
        op_attributes += "\ttemplate_ext: Sequence[str] = ()\n"
        op_attributes += '\tui_color = "#f4a460"\n'
        return op_attributes

    @staticmethod
    def construct_operator_init(docstring: NumpyDocString) -> str:
        """Construct the operator init code based on the provided NumpyDocString.

        Parameters
        ----------
        docstring : NumpyDocString
            The NumpyDocString object containing the documentation.

        Returns
        -------
        str
            The constructed operator init code.
        """
        op_init = "\tdef __init__(self,*,"
        for param in docstring["Parameters"]:
            op_init += f"{param.name}: Optional[{param.type}] = None,"
        op_init += "datarobot_conn_id: str = DATAROBOT_CONN_ID,\n"
        op_init += "**kwargs: Any) -> None:\n"
        op_init += "\t\tsuper().__init__(**kwargs)\n"
        for param in docstring["Parameters"]:
            op_init += f"\t\tself.{param.name} = {param.name}\n"
        op_init += "\t\tself.datarobot_conn_id = datarobot_conn_id\n"
        return op_init

    @staticmethod
    def construct_operator_execute(
        docstring: NumpyDocString,
        operator_module: str,
        operator_name: str,
        operator_method: str,
    ) -> str:
        """Construct the operator execute code based on the provided NumpyDocString.

        Parameters
        ----------
        docstring : NumpyDocString
            The NumpyDocString object containing the documentation.
        operator_module : str
            The module name.
        operator_name : str
            The name of the operator.
        operator_method : str
            The method name.

        Returns
        -------
        str
            The constructed operator execute code.
        """
        op_execute = "\tdef execute(self, context: Context) -> str:\n"
        op_execute += "\t\t# Initialize DataRobot client\n"
        op_execute += "\t\tDataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()\n\n"
        for param in docstring["Parameters"]:
            op_execute += f"\t\tif self.{param.name} is None:\n"
            op_execute += (
                f'\t\t\traise ValueError("{param.name} is required for {operator_name}.")\n'
            )
        op_execute += "\n"
        op_execute += f"\t\tresult = {operator_name}.{operator_method}(\n\t\t"
        for param in docstring["Parameters"]:
            op_execute += f"self.{param.name}, "
        op_execute += "\n\t)\n"
        op_execute += "\t\treturn result.id\n"
        return op_execute


parser = argparse.ArgumentParser(description="Generate code for DataRobot provider")
parser.add_argument("--whitelist", type=str, help="Whitelist file path")
parser.add_argument("--output_folder", type=str, help="Output folder path")


def to_snake_case(camel_case_str: str) -> str:
    return "".join(["_" + i.lower() if i.isupper() else i for i in camel_case_str]).lstrip("_")


def main(whitelist_path: Path, output_folder: Path) -> None:
    # load whitelist yaml
    with open(whitelist_path, "r") as f:
        whitelist = yaml.safe_load(f)

    # generate operators
    generator = GenerateOperators(whitelist)
    generated_code = generator.generate()
    modules: dict[str, dict[str, str]] = defaultdict(dict)
    for operator_key, code in generated_code.items():
        _, module_name, operator_name = operator_key.split(".")
        modules[module_name][operator_name] = code

    # write generated code to files
    for module_name, operators in modules.items():
        module_folder = output_folder / module_name
        module_folder.mkdir(parents=True, exist_ok=True)
        for operator_name, code in operators.items():
            # transform operator name from camel case to snake case
            operator_file_name = to_snake_case(operator_name)
            with open(module_folder / f"{operator_file_name}.py", "w") as f:
                f.write(code)

        # write init file, one line for each module
        with open(module_folder / "__init__.py", "w") as f:
            for operator_name in operators.keys():
                operator_file_name = to_snake_case(operator_name)
                f.write(f"from .{operator_file_name} import {operator_name} as {operator_name}\n")


if __name__ == "__main__":
    args = parser.parse_args()
    main(Path(args.whitelist), Path(args.output_folder))
