# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import datetime
import logging
from collections.abc import Sequence
from hashlib import sha256
from typing import Any
from typing import List
from typing import Optional
from typing import cast

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from datarobot.enums import FileLocationType
from datarobot.models.recipe_operation import RandomSamplingOperation
from datarobot.utils.source import parse_source_type

from datarobot_provider.hooks.connections import JDBCDataSourceHook
from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator
from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator

# Time in seconds after which dataset uploading is considered unsuccessful.
DATAROBOT_MAX_WAIT_SEC = 3600


class UploadDatasetOperator(BaseUseCaseEntityOperator):
    """
    Uploading local file to DataRobot AI Catalog and return Dataset ID.
    :param file_path: The path to the file.
    :type file_path: str, optional
    :param file_path_param: DEPRECATED. Name of the parameter in the configuration to use as file_path, defaults to `dataset_file_path`
    :type file_path_param: str, optional
    :param use_case_id: ID of the use case to add the dataset into.
    :type use_case_id: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["file_path", "file_path_param", "use_case_id"]

    def __init__(
        self,
        *,
        file_path: str = "{{ params.dataset_file_path | default('') }}",  # Don't use any *default* after *dataset_file_path* is finally removed.
        file_path_param: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.file_path_param = file_path_param

        if self.file_path_param is not None:
            self._file_path_param_is_deprecated()

    def execute(self, context: Context) -> str:
        # Upload Dataset to AI Catalog
        self.log.info("Upload Dataset to AI Catalog")
        if not self.file_path and self.file_path_param:
            self._file_path_param_is_deprecated()
            self.file_path = context["params"][self.file_path_param]

        source_type = parse_source_type(self.file_path)
        if source_type == FileLocationType.URL:
            ai_catalog_dataset: dr.Dataset = dr.Dataset.create_from_url(
                url=self.file_path, max_wait=DATAROBOT_MAX_WAIT_SEC
            )

        elif source_type == FileLocationType.PATH:
            ai_catalog_dataset = dr.Dataset.create_from_file(
                file_path=self.file_path, max_wait=DATAROBOT_MAX_WAIT_SEC
            )

        else:
            raise AirflowException(f"Unexpected file_path type: {source_type}")

        self.log.info(f"Dataset created: dataset_id={ai_catalog_dataset.id}")
        self.add_into_use_case(ai_catalog_dataset, context=context)

        return ai_catalog_dataset.id

    def _file_path_param_is_deprecated(self) -> None:
        self.log.warning(
            "**file_path_param** is deprecated. "
            f"Use `file_path={{{{ params.{self.file_path_param} }}}}` instead."
        )


class UpdateDatasetFromFileOperator(BaseDatarobotOperator):
    """
    Operator that creates a new Dataset version from a file.
    Returns when the new dataset version has been successfully uploaded.

    :param dataset_id: DataRobot AI Catalog dataset ID
    :type dataset_id: str, optional
    :param dataset_id_param: Name of the parameter in the configuration to use as dataset_id, defaults to `training_dataset_id`
    :type dataset_id_param: str, optional
    :param file_path: The path to the file to upload
    :type file_path: str, optional
    :param file_path_param: Name of the parameter in the configuration to use as file_path, defaults to `dataset_file_path`
    :type file_path_param: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset version ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "dataset_id",
        "dataset_id_param",
        "file_path",
        "file_path_param",
    ]

    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        dataset_id_param: str = "training_dataset_id",
        file_path: Optional[str] = None,
        file_path_param: str = "dataset_file_path",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.dataset_id_param = dataset_id_param
        self.file_path = file_path
        self.file_path_param = file_path_param

    def execute(self, context: Context) -> str:
        # If dataset_id not provided in constructor, then using dataset_id_param from json config
        dataset_id = (
            self.dataset_id
            if self.dataset_id is not None
            else context["params"][self.dataset_id_param]
        )

        # The path to the file.
        file_path = (
            self.file_path
            if self.file_path is not None
            else context["params"][self.file_path_param]
        )

        self.log.info(f"Update Dataset {dataset_id} in AI Catalog from the local file: {file_path}")
        ai_catalog_dataset = dr.Dataset.create_version_from_file(
            dataset_id=dataset_id,
            file_path=file_path,
            max_wait=DATAROBOT_MAX_WAIT_SEC,
        )
        self.log.info(
            f"Dataset updated: dataset_id={ai_catalog_dataset.id}, dataset_version_id:{ai_catalog_dataset.version_id}"
        )

        return ai_catalog_dataset.version_id


class CreateDatasetFromDataStoreOperator(BaseDatarobotOperator):
    """
    Loading dataset from JDBC Connection to DataRobot AI Catalog and return Dataset ID.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = []

    def execute(self, context: Context) -> str:
        # Fetch stored JDBC Connection with credentials
        _, credential_data, data_store = JDBCDataSourceHook(
            datarobot_credentials_conn_id=context["params"]["datarobot_jdbc_connection"]
        ).run()

        dataset_name = context["params"]["dataset_name"]

        data_source = None

        for dr_source_item in dr.DataSource.list():
            if dr_source_item.canonical_name == dataset_name:
                data_source = dr_source_item
                self.log.info(f"Found existing DataSource:{dataset_name}, id={data_source.id}")
                break

        # Creating DataSourceParameters:
        if "query" in context["params"] and context["params"]["query"]:
            # using sql statement if provided:
            params = dr.DataSourceParameters(query=context["params"]["query"])
        else:
            # otherwise using schema and table:
            params = dr.DataSourceParameters(
                schema=context["params"]["table_schema"], table=context["params"]["table_name"]
            )

        if data_source is None:
            # Adding data_store_id to params (required for DataSource creation):
            params.data_store_id = data_store.id
            # Creating DataSource using params with data_store_id
            self.log.info(f"Creating DataSource: {dataset_name}")
            data_source = dr.DataSource.create(
                data_source_type="jdbc", canonical_name=dataset_name, params=params
            )
            self.log.info(f"DataSource:{dataset_name} successfully created, id={data_source.id}")

        # Checking if there are any changes in params:
        elif params != data_source.params:
            # If params in changed, updating data source:
            self.log.info(f"Updating DataSource:{dataset_name} with new params")
            data_source.update(canonical_name=dataset_name, params=params)
            self.log.info(f"DataSource:{dataset_name} successfully updated, id={data_source.id}")

        self.log.info(f"Creating Dataset from DataSource: {dataset_name}")
        ai_catalog_dataset: dr.Dataset = dr.Dataset.create_from_data_source(
            data_source_id=data_source.id,
            credential_data=credential_data,
            persist_data_after_ingestion=context["params"]["persist_data_after_ingestion"],
            do_snapshot=context["params"]["do_snapshot"],
            max_wait=DATAROBOT_MAX_WAIT_SEC,
        )
        self.log.info(f"Dataset created: dataset_id={ai_catalog_dataset.id}")
        return ai_catalog_dataset.id


class CreateDatasetFromRecipeOperator(BaseUseCaseEntityOperator):
    """Create a dataset based on a wrangling recipe.
    The dataset can be dynamic or a snapshot depending on the mandatory *do_snapshot* parameter.
    The dataset is added into the Use Case if use_case_id is specified.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :param recipe_id: Wrangling or Feature Discovery Recipe Id
    :type recipe_id: str
    :param do_snapshot: *True* to download and store whole dataframe into DataRobot AI Catalog. *False* to create a dynamic dataset.
    :type do_snapshot: bool
    :param dataset_name: Name of the new dataset.
    :type dataset_name: str
    :param materialization_catalog: Data store catalog (database) to upload the wrangled data into.
    :type materialization_catalog: str
    :param materialization_schema: The database schema to upload the wrangled data into.
    :type materialization_schema: str
    :param materialization_table: The database table to upload the wrangled data into.
    :type materialization_table: str
    :param use_case_id: ID of the use case to add the dataset into.
    :type use_case_id: str or None
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    template_fields = [
        "recipe_id",
        "use_case_id",
        "dataset_name",
        "materialization_catalog",
        "materialization_schema",
        "materialization_table",
    ]

    def __init__(
        self,
        *,
        recipe_id: str,
        do_snapshot: bool,
        dataset_name: Optional[str] = "{{ params.dataset_name | default('') }}",
        materialization_catalog: Optional[
            str
        ] = "{{ params.materialization_catalog | default('') }}",
        materialization_schema: Optional[str] = "{{ params.materialization_schema | default('') }}",
        materialization_table: Optional[str] = "{{ params.materialization_table | default('') }}",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.recipe_id = recipe_id
        self.do_snapshot = do_snapshot
        self.dataset_name = dataset_name
        self.materialization_catalog = materialization_catalog
        self.materialization_schema = materialization_schema
        self.materialization_table = materialization_table

    def _get_materialization_destination(
        self,
    ) -> Optional[dr.models.dataset.MaterializationDestination]:
        if self.materialization_table:
            return dr.models.dataset.MaterializationDestination(
                catalog=self.materialization_catalog or None,  # type: ignore[typeddict-item]
                schema=self.materialization_schema or None,  # type: ignore[typeddict-item]
                table=self.materialization_table,
            )

        return None

    def _get_dataset_name(
        self,
        materialization_destination: Optional[dr.models.dataset.MaterializationDestination],
    ) -> Optional[str]:
        return self.dataset_name or (
            materialization_destination and materialization_destination["table"]
        )

    def execute(self, context: Context) -> str:
        recipe = dr.models.Recipe.get(self.recipe_id)
        if recipe.dialect == dr.enums.DataWranglingDialect.SPARK and not self.do_snapshot:
            raise AirflowException(
                "Dynamic datasets are not suitable for 'spark' recipes. "
                "Please, either specify do_snapshot=True for the operator or use another recipe."
            )

        materialization_destination = self._get_materialization_destination()
        dataset_name = self._get_dataset_name(materialization_destination)

        dataset: dr.Dataset = dr.Dataset.create_from_recipe(
            recipe,
            name=dataset_name,
            do_snapshot=self.do_snapshot,
            persist_data_after_ingestion=True,
            materialization_destination=materialization_destination,
        )

        logging.info(
            '%s dataset "%s" created.',
            "Snapshot" if self.do_snapshot else "Dynamic",
            dataset.name,
        )
        self.add_into_use_case(dataset, context=context)

        return dataset.id


class CreateDatasetVersionOperator(BaseDatarobotOperator):
    """
    Creating new version of existing dataset in AI Catalog and return dataset version ID.

    :param dataset_id: DataRobot AI Catalog dataset ID
    :type dataset_id: str
    :param datasource_id: existing DataRobot datasource ID
    :type datasource_id: str
    :param credential_id: existing DataRobot credential ID
    :type credential_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset version ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["dataset_id", "datasource_id", "credential_id"]

    def __init__(
        self,
        *,
        dataset_id: str,
        datasource_id: str,
        credential_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.datasource_id = datasource_id
        self.credential_id = credential_id

    def execute(self, context: Context) -> str:
        self.log.debug(
            f"Creation new version of dataset: dataset_id={self.dataset_id}, "
            f"using datasource: datasource_id={self.datasource_id}, "
            f"with credentials: credentials_id={self.credential_id}."
        )

        ai_catalog_dataset = dr.Dataset.create_version_from_data_source(
            dataset_id=self.dataset_id,
            data_source_id=self.datasource_id,
            credential_id=self.credential_id,
            max_wait=DATAROBOT_MAX_WAIT_SEC,
        )

        self.log.info(
            f"Dataset version created: dataset_id={ai_catalog_dataset.id},"
            f" version_id={ai_catalog_dataset.version_id}"
        )

        return ai_catalog_dataset.version_id


class CreateDatasetFromProjectOperator(BaseDatarobotOperator):
    """
    Create a new AI Catalog Dataset from existing project data.
    :param project_id: DataRobot project ID
    :type project_id: str
    :param datasource_id: existing DataRobot datasource ID
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    template_fields: Sequence[str] = ["project_id"]

    def __init__(
        self,
        *,
        project_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id

    def execute(self, context: Context) -> str:
        dataset: dr.Dataset = dr.Dataset.create_from_project(project_id=self.project_id)
        self.log.info(f"Dataset created: dataset_id={dataset.id}")
        return dataset.id


class CreateOrUpdateDataSourceOperator(BaseDatarobotOperator):
    """
    Get an existing data source by name and update it if any of *table_schema*, *table_name*, *query* are specified.
    Create a new data source if there is no existing one with the specified name.

    :param data_store_id: DataRobot data store ID
    :type data_store_id: str
    :param dataset_name: Data source canonical name to create or update.
    :type dataset_name: Optional[str]
    :param table_schema: Database schema name.
    :type table_schema: Optional[str]
    :param table_name: Database table name.
    :type table_name: Optional[str]
    :param query: Database table name.
    :type query: Optional[str]

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog data source ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "data_store_id",
        "dataset_name",
        "table_name",
        "table_schema",
        "query",
    ]

    def __init__(
        self,
        *,
        data_store_id: str,
        dataset_name: Optional[str] = "{{ params.get('dataset_name', '') }}",
        table_schema: Optional[str] = "{{ params.get('table_schema', '') }}",
        table_name: Optional[str] = "{{ params.get('table_name', '') }}",
        query: Optional[str] = "{{ params.get('query', '') }}",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.data_store_id = data_store_id
        self.dataset_name = dataset_name
        self.table_schema = table_schema
        self.table_name = table_name
        self.query = query

    def execute(self, context: Context) -> Optional[str]:
        self.log.debug(f"Trying to get existing DataStore by data_store_id={self.data_store_id}")
        data_store = dr.DataStore.get(data_store_id=self.data_store_id)
        self.log.debug(f"Found existing DataStore: {data_store.canonical_name}, id={data_store.id}")

        if not self.dataset_name:
            self.dataset_name = self._get_default_data_source_name(cast(str, data_store.id))
            self.log.info("Use default name for the data source: %s", self.dataset_name)

        # Creating DataSourceParameters:
        if self.query:
            # using sql statement if provided:
            params = dr.DataSourceParameters(query=self.query)
        elif self.table_name:
            # otherwise using schema and table:
            params = dr.DataSourceParameters(schema=self.table_schema, table=self.table_name)

        else:
            # or only search by name.
            params = None

        self.log.debug(f"Trying to get existing DataSource by name={self.dataset_name}")
        for data_source in dr.DataSource.list():
            if data_source.canonical_name == self.dataset_name:
                self.log.info(f"Found existing DataSource:{self.dataset_name}, id={data_source.id}")
                if params is not None and params != data_source.params:
                    # If params in changed, updating data source:
                    self.log.info(f"Updating DataSource:{self.dataset_name} with new params")
                    data_source.update(canonical_name=self.dataset_name, params=params)
                    self.log.info(
                        f"DataSource:{self.dataset_name} successfully updated, id={data_source.id}"
                    )
                break
        else:
            if params is None:
                raise AirflowException(
                    f"{self.dataset_name} data source was not found. "
                    "Set *table_schema* and *table_name* or a *query* parameter "
                    "to create a new one instead."
                )

            # Adding data_store_id to params (required for DataSource creation):
            params.data_store_id = data_store.id
            # Creating DataSource using params with data_store_id
            self.log.info(f"Creating DataSource: {self.dataset_name}")
            data_source = dr.DataSource.create(
                data_source_type="jdbc", canonical_name=self.dataset_name, params=params
            )
            self.log.info(
                f"DataSource:{self.dataset_name} successfully created, id={data_source.id}"
            )

        return data_source.id

    def _get_default_data_source_name(self, data_store_id: str) -> str:
        """Build default name based on the data source params."""
        parts = ["Airflow", data_store_id]

        if self.query:
            parts += ["q", sha256(self.query.encode()).hexdigest()]

        else:
            parts_additions = ["t"]
            if self.table_schema:
                parts_additions.append(self.table_schema)
            if self.table_name:
                parts_additions.append(self.table_name)
            parts += parts_additions

        return "-".join(parts)


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
        dialect: dr.enums.DataWranglingDialect,
        recipe_name: Optional[str] = None,
        recipe_description: Optional[str] = "Created with Apache-Airflow",
        operations: Optional[List[dict]] = None,
        downsampling_directive: Optional[dr.enums.DownsamplingOperations] = None,
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
        self.operations = operations
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

            dataset = dr.Dataset.get(self.dataset_id)
            recipe = dr.models.Recipe.from_dataset(
                use_case, dataset, dialect=dr.enums.DataWranglingDialect(self.dialect)
            )

        elif self.data_store_id:
            if not self.table_name:
                raise AirflowException(
                    "*table_name* parameter must be specified "
                    "when working with a data store (db connection)."
                )

            self.log.info("Working with data_store_id=%s", self.data_store_id)
            data_store = dr.DataStore.get(self.data_store_id)
            if not (
                data_store.type and data_store.type in iter(dr.enums.DataWranglingDataSourceTypes)
            ):
                raise AirflowException(f"Unexpected data store type: {data_store.type}")

            data_source_canonical_name = self._generate_data_source_canonical_name()

            recipe = dr.models.Recipe.from_data_store(
                use_case,
                data_store,
                data_source_type=dr.enums.DataWranglingDataSourceTypes(data_store.type),
                dialect=dr.enums.DataWranglingDialect(self.dialect),
                data_source_inputs=[
                    dr.models.DataSourceInput(
                        canonical_name=data_source_canonical_name,
                        schema=self.table_schema,
                        table=self.table_name,
                        sampling=RandomSamplingOperation(1000, 0),
                    )
                ],
            )

        else:
            raise AirflowException("Please specify either dataset_id or data_store_id to wrangle.")

        logging.info(
            '%s recipe id=%s created in use case "%s". Configuring...',
            self.dialect,
            recipe.id,
            use_case.name,
        )

        if self.operations:
            client_operations = [
                dr.models.recipe.WranglingOperation.from_data(x) for x in self.operations
            ]
            dr.models.Recipe.set_operations(recipe.id, client_operations)
            logging.info("%d operations set.", len(client_operations))

        if self.downsampling_directive is not None:
            client_downsampling = dr.models.recipe.DownsamplingOperation(
                directive=dr.enums.DownsamplingOperations(self.downsampling_directive),
                arguments=self.downsampling_arguments,
            )
            dr.models.Recipe.update_downsampling(recipe.id, client_downsampling)
            logging.info("%s dowsnsampling set.", self.downsampling_directive)

        if self.recipe_name or self.recipe_description:
            data = {"description": self.recipe_description}
            if self.recipe_name:
                data["name"] = self.recipe_name

            dr.client.get_client().patch(f"recipes/{recipe.id}/", json=data)
            logging.info("Recipe name/description set.")

        logging.info("Recipe id=%s is ready.", recipe.id)
        return recipe.id

    def _generate_data_source_canonical_name(self) -> str:
        base_name = f"{self.table_name}-{datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}"

        if self.table_schema:
            base_name = f"{self.table_schema}-{base_name}"

        return f"Airflow:{base_name}"
