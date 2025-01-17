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

from datarobot_provider.operators.monitoring import GetAccuracyOperator
from datarobot_provider.operators.monitoring import GetServiceStatsOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "mlops"],
)
def deployment_service_stats_and_accuracy(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    service_stats_op = GetServiceStatsOperator(
        task_id="get_service_stats",
        deployment_id=deployment_id,
    )

    get_accuracy_op = GetAccuracyOperator(task_id="get_accuracy", deployment_id=deployment_id)

    @task(task_id="example_accuracy_check_python")
    def example_metrics_processing(model_service_stat, model_accuracy):
        """Example of custom logic based on metrics from the deployment."""

        # Put your service stat processing logic here:
        current_model_id = model_accuracy["model_id"]

        total_predictions = model_service_stat["metrics"]["totalPredictions"]
        print(f"model_id:{current_model_id}, total_predictions:{total_predictions}")
        print(f"model_id:{current_model_id}, model_accuracy: {model_accuracy}")

        return total_predictions

    metrics_processing = example_metrics_processing(
        model_service_stat=service_stats_op.output, model_accuracy=get_accuracy_op.output
    )

    (service_stats_op, get_accuracy_op) >> metrics_processing


deployment_metrics_dag = deployment_service_stats_and_accuracy()

if __name__ == "__main__":
    deployment_metrics_dag.test()
