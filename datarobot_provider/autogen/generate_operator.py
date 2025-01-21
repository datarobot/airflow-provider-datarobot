import importlib

import black
from numpydoc.docscrape import NumpyDocString


class GenerateOperators:
    def __init__(self, whitelist):
        self.whitelist = whitelist

    def generate(self):
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

    @staticmethod
    def format_code_block_with_black(code_block: str) -> str:
        return str(black.format_file_contents(code_block, fast=False, mode=black.FileMode()))

    def generate_operator_for_method(self, module_key, module_obj_key, method, method_docstring):
        print(f"Generating operator for {module_obj_key} {method}")
        doc = NumpyDocString(str(method_docstring))
        operator_name = f"{module_obj_key}{method.title()}Operator"
        op_python_code = (
            f"from {module_key} import {module_obj_key}\n\n"
            + f"class {operator_name}(BaseOperator):\n"
            + self.construct_operator_docstring(doc)
            + "\n\n"
            + self.construct_operator_attibutes(doc)
            + "\n\n"
            + self.construct_operator_init(doc)
            + "\n\n"
            + self.construct_operator_execute(doc, module_key, module_obj_key, method)
            + "\n"
        )
        op_python_code = self.format_code_block_with_black(op_python_code)
        return op_python_code

    def construct_operator_docstring(self, docstring: NumpyDocString) -> str:
        op_docstring = f'\t"""\t{" ".join(docstring["Summary"])}\n\n'
        for param in docstring["Parameters"]:
            op_docstring += f"\t:param {param.name}: {' '.join(param.desc)}\n"
            op_docstring += f"\t:type {param.name}: {param.type}\n"
        for returns in docstring["Returns"]:
            op_docstring += f"\t:return: {returns.name}\n"
            op_docstring += f"\t:rtype {returns.type}\n"
        op_docstring += '\t"""\n'
        return op_docstring

    def construct_operator_attibutes(self, docstring: NumpyDocString) -> str:
        param_names = [param.name for param in docstring["Parameters"]]
        param_types = [param.type for param in docstring["Parameters"]]
        op_attributes = f"\ttemplate_fields: Sequence[str] = {param_names}\n"
        op_attributes += (
            f"\ttemplate_fields_renderers: dict[{', '.join(map(str, param_types))}] = {{}}\n"
        )
        op_attributes += "\ttemplate_ext: Sequence[str] = ()\n"
        op_attributes += '\tui_color = "#f4a460"\n'
        return op_attributes

    def construct_operator_init(self, docstring: NumpyDocString) -> str:
        op_init = "\tdef __init__(self,*,"
        for param in docstring["Parameters"]:
            op_init += f"{param.name}: {param.type} = None,"
        op_init += 'datarobot_conn_id: str = "datarobot_default",\n'
        op_init += "**kwargs: Any) -> None:\n"
        op_init += "\t\tsuper().__init__(**kwargs)\n"
        for param in docstring["Parameters"]:
            op_init += f"\t\tself.{param.name} = {param.name}\n"
        op_init += "\t\tself.datarobot_conn_id = datarobot_conn_id\n"
        return op_init

    def construct_operator_execute(
        self,
        docstring: NumpyDocString,
        operator_module: str,
        operator_name: str,
        operator_method: str,
    ) -> str:
        op_execute = "\tdef execute(self, context: Context) -> str:\n"
        op_execute += "\t\t# Initialize DataRobot client\n"
        op_execute += "\t\tDataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()\n\n"
        for param in docstring["Parameters"]:
            op_execute += f"\t\tif self.{param.name} is None:\n"
            op_execute += (
                f'\t\t\traise ValueError("{param.name} is required for {operator_name}.")\n'
            )
        op_execute += "\n"
        op_execute += f"\t\tresult = {operator_module}.{operator_name}.{operator_method}(\n\t\t"
        for param in docstring["Parameters"]:
            op_execute += f"self.{param.name}, "
        op_execute += "\n\t)\n"
        op_execute += "\t\treturn result.id\n"
        return op_execute
