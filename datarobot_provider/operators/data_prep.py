# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

import datetime
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from datarobot import DataStore
from datarobot.enums import DataWranglingDataSourceTypes
from datarobot.enums import DataWranglingDialect
from datarobot.enums import DataWranglingSnapshotPolicy
from datarobot.enums import DownsamplingOperations
from datarobot.enums import RecipeInputType
from datarobot.models import DataSourceInput
from datarobot.models import JDBCTableDataSourceInput
from datarobot.models import Recipe
from datarobot.models import RecipeDatasetInput
from datarobot.models.dataset import Dataset
from datarobot.models.recipe_operation import DownsamplingOperation
from datarobot.models.recipe_operation import RandomSamplingOperation
from datarobot.models.recipe_operation import WranglingOperation

from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator


class CreateWranglingRecipeOperator(BaseUseCaseEntityOperator):
    """Create a Wrangling Recipe

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :param use_case_id: Use Case ID to create the recipe in.
    :param dataset_id: The dataset to wrangle
    :param dialect: SQL dialect to apply while wrangling.
    :param recipe_name: New recipe name.
    :param recipe_description: New recipe description.
    :param operations: Wrangling operations to apply.
    :param downsampling_directive: Downsampling method to apply. *None* for non downsampling.
    :param downsampling_arguments_param: Downsampling arguments.

    """

    template_fields: Sequence[str] = [
        "use_case_id",
        "dataset_id",
        "data_store_id",
        "table_schema",
        "table_name",
        "dialect",
        "recipe_name",
        "recipe_description",
        "operations",
        "downsampling_directive",
        "downsampling_arguments",
    ]
    template_fields_renderers: dict[str, str] = {
        "use_case_id": "string",
        "dataset_id": "string",
        "data_store_id": "string",
        "table_schema": "string",
        "table_name": "string",
        "dialect": "string",
        "recipe_name": "string",
        "recipe_description": "string",
        "operations": "json",
        "downsampling_directive": "string",
        "downsampling_arguments": "json",
    }

    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        data_store_id: Optional[str] = None,
        table_schema: Optional[str] = "{{ params.get('table_schema', '') }}",
        table_name: Optional[str] = "{{ params.get('table_name', '') }}",
        dialect: DataWranglingDialect,
        recipe_name: Optional[str] = None,
        recipe_description: Optional[str] = "Created with Apache-Airflow",
        operations: Optional[List[dict]] = None,
        downsampling_directive: Optional[DownsamplingOperations] = None,
        downsampling_arguments: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.data_store_id = data_store_id
        self.table_schema = table_schema
        self.table_name = table_name
        self.dialect = dialect
        self.recipe_name = recipe_name
        self.recipe_description = recipe_description
        self.operations = operations or []
        self.downsampling_directive = downsampling_directive
        self.downsampling_arguments = downsampling_arguments

    def validate(self) -> None:
        if self.dataset_id and self.data_store_id:
            raise AirflowException(
                "You have to specify either dataset_id or data_store_id. Not both."
            )

    def execute(self, context: Context) -> str:
        use_case: dr.UseCase = self.get_use_case(context, required=True)  # type: ignore[assignment]

        if self.dataset_id:
            self.log.info("Working with dataset_id=%s", self.dataset_id)

            dataset = Dataset.get(self.dataset_id)
            recipe = Recipe.from_dataset(
                use_case, dataset, dialect=DataWranglingDialect(self.dialect)
            )

        elif self.data_store_id:
            if not self.table_name:
                raise AirflowException(
                    "*table_name* parameter must be specified "
                    "when working with a data store (db connection)."
                )

            self.log.info("Working with data_store_id=%s", self.data_store_id)
            data_store = DataStore.get(self.data_store_id)
            if not (data_store.type and data_store.type in list(DataWranglingDataSourceTypes)):
                raise AirflowException(f"Unexpected data store type: {data_store.type}")

            data_source_canonical_name = self._generate_data_source_canonical_name()

            recipe = Recipe.from_data_store(
                use_case,
                data_store,
                data_source_type=DataWranglingDataSourceTypes(data_store.type),
                dialect=DataWranglingDialect(self.dialect),
                data_source_inputs=[
                    DataSourceInput(
                        canonical_name=data_source_canonical_name,
                        schema=self.table_schema,
                        table=self.table_name,
                        sampling=RandomSamplingOperation(1000, 0),
                    )
                ],
            )

        else:
            raise AirflowException("Please specify either dataset_id or data_store_id to wrangle.")

        self.log.info(
            '%s recipe id=%s created in use case "%s". Configuring...',
            self.dialect,
            recipe.id,
            use_case.name,
        )

        if self.operations:
            self._set_operations(recipe)
            self.log.info("%d operations set.", len(self.operations))

        if self.downsampling_directive is not None:
            client_downsampling = DownsamplingOperation(
                directive=DownsamplingOperations(self.downsampling_directive),
                arguments=self.downsampling_arguments,
            )
            Recipe.update_downsampling(recipe.id, client_downsampling)
            self.log.info("%s dowsnsampling set.", self.downsampling_directive)

        if self.recipe_name or self.recipe_description:
            data = {"description": self.recipe_description}
            if self.recipe_name:
                data["name"] = self.recipe_name

            dr.client.get_client().patch(f"recipes/{recipe.id}/", json=data)
            self.log.info("Recipe name/description set.")

        self.log.info("Recipe id=%s is ready.", recipe.id)
        return recipe.id

    def _generate_data_source_canonical_name(self) -> str:
        base_name = f"{self.table_name}-{datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}"

        if self.table_schema:
            base_name = f"{self.table_schema}-{base_name}"

        return f"Airflow:{base_name}"

    def _set_operations(self, recipe: Recipe) -> None:
        secondary_inputs: Dict[str, Union[JDBCTableDataSourceInput, RecipeDatasetInput]] = {}

        if recipe.inputs[0].input_type == RecipeInputType.DATASOURCE:
            data_store_id = recipe.inputs[0].data_store_id  # type: ignore[union-attr]
            primary_dataset_id = None
        else:
            data_store_id = None
            primary_dataset_id = recipe.inputs[0].dataset_id

        for operation_data in self.operations:
            if operation_data["directive"] == "join":
                if operation_data["arguments"].get("rightDataSourceId"):
                    secondary_inputs[operation_data["arguments"]["rightDataSourceId"]] = (
                        JDBCTableDataSourceInput(
                            input_type=RecipeInputType.DATASOURCE,
                            data_store_id=data_store_id,  # type: ignore[arg-type]
                            data_source_id=operation_data["arguments"]["rightDataSourceId"],
                        )
                    )

                else:
                    if not operation_data["arguments"].get("rightDatasetVersionId"):
                        dataset = Dataset.get(operation_data["arguments"]["rightDatasetId"])
                        operation_data["arguments"]["rightDatasetVersionId"] = dataset.version_id

                    if operation_data["arguments"]["rightDatasetId"] != primary_dataset_id:
                        secondary_inputs[operation_data["arguments"]["rightDatasetVersionId"]] = (
                            RecipeDatasetInput(
                                input_type=RecipeInputType.DATASET,
                                dataset_id=operation_data["arguments"]["rightDatasetId"],
                                dataset_version_id=operation_data["arguments"][
                                    "rightDatasetVersionId"
                                ],
                                snapshot_policy=DataWranglingSnapshotPolicy.FIXED,
                            )
                        )

        if secondary_inputs:
            inputs = recipe.inputs + list(secondary_inputs.values())
            Recipe.set_inputs(recipe.id, inputs)

        client_operations = [WranglingOperation.from_data(x) for x in self.operations]

        Recipe.set_operations(recipe.id, client_operations)


class CreateFeatureDiscoveryRecipeOperator(BaseUseCaseEntityOperator):
    """
    Create a Feature Discovery recipe.

    :param dataset_id: DataRobot Dataset Catalog ID to use as the primary dataset
    :type dataset_id: str
    :param use_case_id: ID of the use case to add the recipe to
    :type use_case_id: str
    :param dataset_definitions: list of dataset definitions
        Each element is a dict retrieved from DatasetDefinitionOperator operator
    :type dataset_definitions: Iterable[dict]
    :param relationships: list of relationships
        Each element is a dict retrieved from DatasetRelationshipOperator operator
    :type relationships: Iterable[dict]
    :param feature_discovery_settings: list of feature discovery settings, optional
        If not provided, it will be retrieved from DAG configuration params otherwise default
        settings will be used.
    :type feature_discovery_settings: Iterable[dict]
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot Recipe ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "dataset_id",
        "use_case_id",
        "dataset_definitions",
        "relationships",
        "feature_discovery_settings",
    ]

    def __init__(
        self,
        *,
        dataset_id: str,
        dataset_definitions: Iterable[dict],
        relationships: Iterable[dict],
        feature_discovery_settings: Optional[Iterable[dict]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.dataset_definitions = dataset_definitions
        self.relationships = relationships
        self.feature_discovery_settings = feature_discovery_settings

    def execute(self, context: Context) -> str:
        use_case: dr.UseCase = self.get_use_case(context, required=True)  # type: ignore[assignment]
        dataset = dr.Dataset.get(self.dataset_id)

        recipe = dr.models.Recipe.from_dataset(
            use_case,
            dataset,
            recipe_type=dr.enums.RecipeType.FEATURE_DISCOVERY,
        )

        # Get dataset version ID if it isn't defined by the user:
        for dataset_definition in self.dataset_definitions:
            if not dataset_definition.get("catalogVersionId"):
                secondary_dataset = dr.Dataset.get(dataset_definition["catalogId"])
                dataset_definition["catalogVersionId"] = secondary_dataset.version_id

        recipe_id = recipe.id
        recipe_config_id = recipe.settings.relationships_configuration_id  # type: ignore[union-attr]

        # Add secondary dataset configuration information into the Recipe config
        dr.RelationshipsConfiguration(recipe_config_id).replace(
            self.dataset_definitions,
            self.relationships,
            self.feature_discovery_settings,
        )

        self.log.info(f"Feature Discovery recipe created: recipe_id={recipe_id}")

        return recipe_id
