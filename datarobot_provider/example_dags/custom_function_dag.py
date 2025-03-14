# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from airflow.decorators import dag

from datarobot_provider.operators.custom_function import CustomFunctionOperator


def super_func(x, y):
    return x * y


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    schedule=None,
    tags=["example", "custom_function"],
)
def custom_function_dag():
    my_function = CustomFunctionOperator(
        task_id="my_function",
        custom_func=super_func,
        func_params={"x": 2, "y": 3},
    )

    (my_function)


# Instantiate the DAG
custom_function_dag()
