# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import Iterable
from typing import Optional

import datarobot as dr
from airflow.utils.context import Context

from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator

DATAROBOT_MAX_WAIT = 600


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
