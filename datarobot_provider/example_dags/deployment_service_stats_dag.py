# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task

from datarobot_provider.operators.monitoring import GetServiceStatsOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
)
def deployment_service_stats():
    service_stats_op = GetServiceStatsOperator(
        task_id="get_service_stats",
        # you can pass deployment_id from previous operator here:
        deployment_id="put your deployment_id here",
    )

    @task(task_id="example_processing_python")
    def service_stat_processing(model_service_stat):
        """Example of custom logic based on service stats from the deployment."""

        # Put your service stat processing logic here:
        current_model_id = model_service_stat["model_id"]
        total_predictions = model_service_stat["metrics"]["totalPredictions"]
        print(f"model_id:{current_model_id}, total_predictions:{total_predictions}")

        return total_predictions

    example_service_stat_processing = service_stat_processing(
        model_service_stat=service_stats_op.output
    )

    service_stats_op >> example_service_stat_processing


deployment_service_stats_dag = deployment_service_stats()

if __name__ == "__main__":
    deployment_service_stats_dag.test()
