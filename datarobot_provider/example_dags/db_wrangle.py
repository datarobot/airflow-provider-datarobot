import datarobot as dr
from airflow.decorators import dag

from datarobot_provider.operators.ai_catalog import CreateDatasetFromRecipeOperator, \
    CreateOrUpdateDataSourceOperator
from datarobot_provider.operators.ai_catalog import CreateWranglingRecipeOperator
from datarobot_provider.operators.connections import GetDataStoreOperator


@dag(
    tags=["example", "wrangling", "database"],
    params={
        "primary_table": "LENDING_CLUB_TARGET",
        "table_schema": "PUBLIC",
        "table_name": "LENDING_CLUB_TRANSACTIONS",
        "datarobot_connection_name": "<YOUR CONNECTION NAME>",
        "use_case_id": "<YOUR USE CASE ID>",
    },
)
def db_wrangle():
    get_connection = GetDataStoreOperator(task_id="get_connection")

    secondary_table = CreateOrUpdateDataSourceOperator(
        task_id="secondary_table", data_store_id=get_connection.output,
    )

    create_recipe = CreateWranglingRecipeOperator(
        task_id="create_recipe",
        data_store_id=get_connection.output,
        dialect=dr.enums.DataWranglingDialect.SNOWFLAKE,
        table_name='{{ params.primary_table }}',
        table_schema='{{ params.table_schema }}',
        operations=[
            {
              "directive": "join",
              "arguments": {
                "leftKeys": [
                  "CustomerID"
                ],
                "rightKeys": [
                  "CustomerID"
                ],
                "joinType": "left",
                "source": "table",
                "rightDataSourceId": secondary_table.output,
              }
            },
            {
              "directive": "replace",
              "arguments": {
                "origin": "Amount",
                "searchFor": "[$,]",
                "replacement": "",
                "matchMode": "regex",
                "isCaseSensitive": False
              }
            },
            {
              "directive": "compute-new",
              "arguments": {
                "expression": "CAST(\"Amount\" AS Decimal(10, 2))",
                "newFeatureName": "decimal amount"
              }
            },
            {
              "directive": "aggregate",
              "arguments": {
                "groupBy": [
                  "CustomerID",
                  "BadLoan",
                  "date"
                ],
                "aggregations": [
                  {"feature": "decimal amount", "functions": ["sum", "avg","stddev"]}
                ]
              }
            }
          ],
    )

    publish_recipe = CreateDatasetFromRecipeOperator(
        task_id="publish_recipe", recipe_id=create_recipe.output, do_snapshot=False
    )

    get_connection >> secondary_table >> create_recipe >> publish_recipe


db_wrangle()
