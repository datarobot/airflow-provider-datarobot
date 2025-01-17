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

from datarobot_provider.operators.scoring_code import DownloadDeploymentScoringCodeOperator


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=["example", "scoring_code"],
    # Default json config example:
    params={
        "source_code": False,
        "include_agent": False,
        "include_prediction_explanations": False,
        "include_prediction_intervals": False,
        # as an option you can pass base_path here:
        # 'scoring_code_filepath': "/home/airflow/gcs/data/",
    },
)
def download_scoring_code(deployment_id=None):
    if not deployment_id:
        raise ValueError("Invalid or missing `deployment_id` value")

    download_deployment_scoring_code_op = DownloadDeploymentScoringCodeOperator(
        task_id="download_scoring_code_from_deployment",
        # you can pass deployment_id from previous operator here:
        deployment_id=deployment_id,
        # you can pass base_path from previous operator here:
        base_path="/.../models/",
    )

    @task(task_id="example_using_scoring_code")
    def using_scoring_code_example(scoring_code_filepath):
        """Example of using scoring code locally"""

        # Put your logic with the scoring code here:
        # Note that local scoring is not recommended to use because its burden an Airflow worker node.
        # The next scoring code example is only for demo and testing proposes,
        # should be used only with a very small datasets (not higher than a few megabytes).
        # To run this example, DataRobot Prediction Library (https://datarobot.github.io/datarobot-predict)
        # should be installed first.

        input_file = "/.../input_file.csv"
        output_file = "/.../output_predictions.csv"

        import pandas as pd
        from datarobot_predict.scoring_code import ScoringCodeModel

        model = ScoringCodeModel(scoring_code_filepath)
        input_df = pd.read_csv(input_file)
        output_df = model.predict(input_df)
        output_df.to_csv(output_file)

    example_using_scoring_code = using_scoring_code_example(
        scoring_code_filepath=download_deployment_scoring_code_op.output,
    )

    download_deployment_scoring_code_op >> example_using_scoring_code


download_scoring_code_dag = download_scoring_code()

if __name__ == "__main__":
    download_scoring_code_dag.test()
