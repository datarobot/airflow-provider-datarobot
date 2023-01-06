# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import pytest
from airflow.models import DagBag

from datarobot_provider.example_dags.datarobot_score_dag import datarobot_score


@pytest.fixture()
def dagbag(provider_dir):
    return DagBag(dag_folder=f"{str(provider_dir)}/example_dags", include_examples=False)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="datarobot_score")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2


def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag_structure():
    dag = datarobot_score()
    assert_dag_dict_equal(
        {
            "score_predictions": ["check_scoring_complete"],
            "check_scoring_complete": [],
        },
        dag,
    )
