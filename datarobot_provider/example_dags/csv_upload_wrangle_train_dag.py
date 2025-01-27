from airflow.models.dag import dag

from datarobot_provider.operators.ai_catalog import UploadDatasetOperator, CreateWranglingRecipeOperator, CreateDatasetFromRecipeOperator
from datarobot_provider.operators.datarobot import CreateProjectOperator, TrainModelsOperator
import datarobot as dr


@dag(
    tags=["example"],
    schedule=None,
    params={
        'dataset_file_path': 'https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/safer_datasets/lending_club/target.csv',
        'secondary_dataset_path': 'https://s3.us-east-1.amazonaws.com/datarobot_public_datasets/safer_datasets/lending_club/transactions.csv',
        'use_case_id': '<YOUR USE CASE ID GOES HERE>',
        'autopilot_settings': {'target': 'BadLoan'},
        'project_name': 'Airflow Project',
    },
)
def url_upload_wrangle_train():
    primary_upload = UploadDatasetOperator(task_id="primary_upload")
    secondary_upload = UploadDatasetOperator(
        task_id="secondary_upload", file_path_param='secondary_dataset_path'
    )
    create_recipe = CreateWranglingRecipeOperator(
        task_id='create_recipe',
        dataset_id=primary_upload.output,
        dialect=dr.enums.DataWranglingDialect.SPARK,
        operations=[
        {
          "directive": "join",
          "arguments": {
            "leftKeys": ["CustomerID"],
            "rightKeys": ["CustomerID"],
            "joinType": "inner",
            "source": "dataset",
            "rightDatasetId": secondary_upload.output,
          }
        },
        {
          "directive": "replace",
          "arguments": {
            "origin": "Amount",
            "searchFor": "$",
            "replacement": "",
            "matchMode": "partial",
            "isCaseSensitive": False
          }
        },
        {
          "directive": "compute-new",
          "arguments": {
            "expression": "CAST(`Amount` as DECIMAL(10, 2))",
            "newFeatureName": "dec_amount"
          }
        },
        {
          "directive": "aggregate",
          "arguments": {
            "groupBy": ["CustomerID", "BadLoan", "date"],
            "aggregations": [
              {"feature": "dec_amount", "functions": ["sum", "avg", "stddev"]}
            ]
          }
        }
      ]
    )
    publish_recipe = CreateDatasetFromRecipeOperator(recipe_id=create_recipe.output, do_snapshot=True, task_id="publish_recipe")
    create_project = CreateProjectOperator(dataset_id=publish_recipe.output, task_id="create_project")
    train_models = TrainModelsOperator(project_id=create_project.output, task_id="train_models")

    [primary_upload, secondary_upload] >> create_recipe >> publish_recipe >> create_project >> train_models


url_upload_wrangle_train()
