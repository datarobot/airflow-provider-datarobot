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
from datarobot import SNAPSHOT_POLICY

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator
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


class RelationshipsConfigurationOperator(BaseDatarobotOperator):
    """
    Create a Relationships Configuration.

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
    :param max_wait_sec: For some settings, an asynchronous task must be run to analyze the
            dataset.  max_wait governs the maximum time (in seconds) to wait before giving up.
    :type max_wait_sec: int, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot Relationships Configuration ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "dataset_definitions",
        "relationships",
        "feature_discovery_settings",
    ]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        dataset_definitions: Iterable[dict],
        relationships: Iterable[dict],
        feature_discovery_settings: Optional[Iterable[dict]] = None,
        max_wait_sec: int = DATAROBOT_MAX_WAIT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_definitions = dataset_definitions
        self.relationships = relationships
        self.feature_discovery_settings = feature_discovery_settings
        self.max_wait_sec = max_wait_sec

    def execute(self, context: Context) -> None:
        # if feature_discovery_settings not provided, try to get it from DAG configuration params:
        if self.feature_discovery_settings is None:
            self.feature_discovery_settings = context["params"].get(
                "feature_discovery_settings", None
            )

        # Create the relationships' configuration to define the connection between the datasets
        # This will be passed to the DataRobot project configuration for Autopilot
        relationship_config = dr.RelationshipsConfiguration.create(
            dataset_definitions=self.dataset_definitions,
            relationships=self.relationships,
            feature_discovery_settings=self.feature_discovery_settings,
        )

        return relationship_config.id


class DatasetDefinitionOperator(BaseDatarobotOperator):
    """
    Dataset definition for the Feature Discovery

    :param dataset_identifier: Alias of the dataset (used directly as part of the
            generated feature names)
    :type dataset_identifier: str
    :param dataset_id: Identifier of the dataset in DataRobot AI Catalog
    :type dataset_id: str
    :param dataset_version_id: Identifier of the dataset version in DataRobot AI Catalog
    :type dataset_version_id: str, optional
    :param primary_temporal_key: Name of the column indicating time of record creation
    :type primary_temporal_key: str, optional
    :param feature_list_id: Specifies which feature list to use.
    :type feature_list_id: str, optional
    :param snapshot_policy: Policy to use  when creating a project or making predictions.
        If omitted, by default endpoint will use 'latest'.
        Must be one of the following values:
        'specified': Use specific snapshot specified by catalogVersionId
        'latest': Use latest snapshot from the same catalog item
        'dynamic': Get data from the source (only applicable for JDBC datasets)
    :type snapshot_policy: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Dataset definition
    :rtype: dict
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "dataset_identifier",
        "dataset_id",
        "dataset_version_id",
        "primary_temporal_key",
        "feature_list_id",
        "snapshot_policy",
    ]

    def __init__(
        self,
        *,
        dataset_identifier: str,
        dataset_id: Optional[str],
        dataset_version_id: str,
        snapshot_policy: str = SNAPSHOT_POLICY.LATEST,
        feature_list_id: Optional[str] = None,
        primary_temporal_key: Optional[str] = None,
        max_wait_sec: int = DATAROBOT_MAX_WAIT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_identifier = dataset_identifier
        self.dataset_id = dataset_id
        self.dataset_version_id = dataset_version_id
        self.snapshot_policy = snapshot_policy
        self.feature_list_id = feature_list_id
        self.primary_temporal_key = primary_temporal_key
        self.max_wait_sec = max_wait_sec

    def execute(self, context: Context) -> dict:
        dataset_definition = dr.DatasetDefinition(
            identifier=self.dataset_identifier,
            catalog_id=self.dataset_id,
            catalog_version_id=self.dataset_version_id,
            feature_list_id=self.feature_list_id,
            primary_temporal_key=self.primary_temporal_key,
            snapshot_policy=self.snapshot_policy,  # requires a jdbc source
        )

        return dataset_definition.to_payload()


class DatasetRelationshipOperator(BaseDatarobotOperator):
    """
    Relationship between dataset defined in DatasetDefinition

    :param dataset1_identifier: Identifier of the first dataset in this relationship.
        This is specified in the identifier field of dataset_definition structure.
        If None, then the relationship is with the primary dataset.
    :type dataset1_identifier: str, optional
    :param dataset2_identifier: Identifier of the second dataset in this relationship.
        This is specified in the identifier field of dataset_definition schema.
    :type dataset2_identifier: str
    :param dataset1_keys: list of string (max length: 10 min length: 1)
        Column(s) from the first dataset which are used to join to the second dataset
    :type dataset1_keys: list
    :param dataset2_keys: list of string (max length: 10 min length: 1)
        Column(s) from the second dataset that are used to join to the first dataset
    :type dataset2_keys: list
    :param feature_derivation_window_start: How many time units of each dataset's
        primary temporal key into the past relative to the datetimePartitionColumn
        the feature derivation window should begin. Will be a negative integer,
        If present, the feature engineering Graph will perform time-aware joins.
    :type feature_derivation_window_start: int, optional
    :param feature_derivation_window_end: How many time units of each dataset's record
        primary temporal key into the past relative to the datetimePartitionColumn the
        feature derivation window should end.  Will be a non-positive integer, if present.
        If present, the feature engineering Graph will perform time-aware joins.
    :type feature_derivation_window_end: int, optional
    :param feature_derivation_window_time_unit: Time unit of the feature derivation window.
        One of ``datarobot.enums.AllowedTimeUnitsSAFER``
        If present, time-aware joins will be used.
        Only applicable when dataset1_identifier is not provided.
    :type feature_derivation_window_time_unit: int, optional
    :param feature_derivation_windows: List of feature derivation windows settings.
        If present, time-aware joins will be used.
        Only allowed when feature_derivation_window_start,
        feature_derivation_window_end and feature_derivation_window_time_unit are not provided.
    :type feature_derivation_windows: int, optional
    :param prediction_point_rounding: list of dict, or None
        Closest value of prediction_point_rounding_time_unit to round the prediction point
        into the past when applying the feature derivation window. Will be a positive integer,
        if present.Only applicable when dataset1_identifier is not provided.
    :type prediction_point_rounding: list[dict], optional
    :param prediction_point_rounding_time_unit: Time unit of the prediction point rounding.
        One of ``datarobot.enums.AllowedTimeUnitsSAFER`` Only applicable when dataset1_identifier
        is not provided.
    :type prediction_point_rounding_time_unit:  str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Relationship definition
    :rtype: dict
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "dataset1_identifier",
        "dataset2_identifier",
        "dataset1_keys",
        "dataset2_keys",
        "feature_derivation_window_start",
        "feature_derivation_window_end",
        "feature_derivation_window_time_unit",
        "feature_derivation_windows",
        "prediction_point_rounding",
        "prediction_point_rounding_time_unit",
    ]

    def __init__(
        self,
        *,
        dataset2_identifier: str,
        dataset1_keys: list[str],
        dataset2_keys: list[str],
        dataset1_identifier: Optional[str] = None,
        feature_derivation_window_start: Optional[int] = None,
        feature_derivation_window_end: Optional[int] = None,
        feature_derivation_window_time_unit: Optional[int] = None,
        feature_derivation_windows: Optional[list[dict]] = None,
        prediction_point_rounding: Optional[int] = None,
        prediction_point_rounding_time_unit: Optional[str] = None,
        max_wait_sec: int = DATAROBOT_MAX_WAIT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset1_identifier = dataset1_identifier
        self.dataset2_identifier = dataset2_identifier
        self.dataset1_keys = dataset1_keys
        self.dataset2_keys = dataset2_keys
        self.feature_derivation_window_start = feature_derivation_window_start
        self.feature_derivation_window_end = feature_derivation_window_end
        self.feature_derivation_window_time_unit = feature_derivation_window_time_unit
        self.feature_derivation_windows = feature_derivation_windows
        self.prediction_point_rounding = prediction_point_rounding
        self.prediction_point_rounding_time_unit = prediction_point_rounding_time_unit
        self.max_wait_sec = max_wait_sec

    def execute(self, context: Context) -> dict:
        relationship = dr.Relationship(
            dataset1_identifier=self.dataset1_identifier,  # join profile
            dataset2_identifier=self.dataset2_identifier,  # to transactions
            dataset1_keys=self.dataset1_keys,  # on CustomerID
            dataset2_keys=self.dataset2_keys,
            feature_derivation_windows=self.feature_derivation_windows,
            prediction_point_rounding=self.prediction_point_rounding,
            prediction_point_rounding_time_unit=self.prediction_point_rounding_time_unit,
            feature_derivation_window_start=self.feature_derivation_window_start,
            feature_derivation_window_end=self.feature_derivation_window_end,
            feature_derivation_window_time_unit=self.feature_derivation_window_time_unit,
        )

        return relationship.to_payload()
