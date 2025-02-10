# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import pytest
from airflow.models import DagBag


@pytest.fixture()
def dagbag(provider_dir):
    return DagBag(
        dag_folder=f"{str(provider_dir)}/_experimental/example_dags", include_examples=False
    )


@pytest.helpers.register
def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)
