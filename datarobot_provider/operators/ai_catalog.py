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

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datarobot_provider.hooks.connections import JDBCDataSourceHook
from datarobot_provider.hooks.datarobot import DataRobotHook

# Time in seconds after which dataset uploading is considered unsuccessful.
DATAROBOT_MAX_WAIT_SEC = 3600


class UploadDatasetOperator(BaseOperator):
    """
    Uploading local file to DataRobot AI Catalog and return Dataset ID.
    :param file_path: The path to the file.
    :type file_path: str, optional
    :param file_path_param: Name of the parameter in the configuration to use as file_path, defaults to `dataset_file_path`
    :type file_path_param: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "file_path",
        "file_path_param",
    ]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        file_path: Optional[str] = None,
        file_path_param: str = "dataset_file_path",
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.file_path = file_path
        self.file_path_param = file_path_param
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Upload Dataset to AI Catalog
        self.log.info("Upload Dataset to AI Catalog")
        if self.file_path is None:
            self.file_path = context["params"][self.file_path_param]

        ai_catalog_dataset: dr.Dataset = dr.Dataset.create_from_file(
            file_path=self.file_path,
            max_wait=DATAROBOT_MAX_WAIT_SEC,
        )

        self.log.info(f"Dataset created: dataset_id={ai_catalog_dataset.id}")
        return ai_catalog_dataset.id


class UpdateDatasetFromFileOperator(BaseOperator):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        dataset_id_param: str = "training_dataset_id",
        file_path: Optional[str] = None,
        file_path_param: str = "dataset_file_path",
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.dataset_id_param = dataset_id_param
        self.file_path = file_path
        self.file_path_param = file_path_param
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

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


class CreateDatasetFromDataStoreOperator(BaseOperator):
    """
    Loading dataset from JDBC Connection to DataRobot AI Catalog and return Dataset ID.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = []
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

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


class CreateDatasetFromRecipeOperator(BaseOperator):
    """Create a dataset based on a wrangling recipe.
    The dataset can be dynamic or a snapshot depending on the mandatory *do_snapshot* parameter.
    The dataset is added into the Use Case if use_case_id is specified
    in the context parameters.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :param recipe_id: Wrangling or Feature Discovery Recipe Id
    :type recipe_id: str
    :param do_snapshot: *True* to download and store whole dataframe into DataRobot AI Catalog. *False* to create a dynamic dataset.
    :type do_snapshot: bool
    :param dataset_name_param: Name of the parameter in the configuration to use as dataset_name
    :type dataset_name_param: str
    :param materialization_catalog_param: Name of the parameter in the configuration to use as materialization_catalog
    :type materialization_catalog_param: str
    :param materialization_schema_param: Name of the parameter in the configuration to use as materialization_schema
    :type materialization_schema_param: str
    :param materialization_table_param: Name of the parameter in the configuration to use as materialization_table
    :type materialization_table_param: str
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    template_fields = ["recipe_id"]
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        datarobot_conn_id: str = "datarobot_default",
        recipe_id: str,
        do_snapshot: bool,
        dataset_name_param: str = "dataset_name",
        materialization_catalog_param: str = "materialization_catalog",
        materialization_schema_param: str = "materialization_schema",
        materialization_table_param: str = "materialization_table",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        self.recipe_id = recipe_id
        self.do_snapshot = do_snapshot

        self.dataset_name_param = dataset_name_param
        self.materialization_catalog_param = materialization_catalog_param
        self.materialization_schema_param = materialization_schema_param
        self.materialization_table_param = materialization_table_param

        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def _get_materialization_destination(
        self, context: Context
    ) -> Optional[dr.models.dataset.MaterializationDestination]:
        if context["params"].get(self.materialization_table_param):
            return dr.models.dataset.MaterializationDestination(
                catalog=context["params"].get(self.materialization_catalog_param),  # type: ignore[typeddict-item]
                schema=context["params"].get(self.materialization_schema_param),  # type: ignore[typeddict-item]
                table=context["params"].get(self.materialization_table_param),  # type: ignore[typeddict-item]
            )

        return None

    def _get_dataset_name(
        self,
        context: Context,
        materialization_destination: Optional[dr.models.dataset.MaterializationDestination],
    ):
        return context["params"].get(self.dataset_name_param) or (
            materialization_destination and materialization_destination["table"]
        )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        recipe = dr.models.Recipe.get(self.recipe_id)
        if recipe.dialect == dr.enums.DataWranglingDialect.SPARK and not self.do_snapshot:
            raise AirflowException(
                "Dynamic datasets are not suitable for 'spark' recipes. "
                "Please, either specify do_snapshot=True for the operator or use another recipe."
            )

        materialization_destination = self._get_materialization_destination(context)
        dataset_name = self._get_dataset_name(context, materialization_destination)

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

        if context["params"].get("use_case_id"):
            use_case = dr.UseCase.get(use_case_id=context["params"]["use_case_id"])
            use_case.add(dataset)
            logging.info('The dataset is added into use case "%s".', use_case.name)

        else:
            logging.info("New Dataset won't belong to any use case.")

        return dataset.id


class CreateDatasetVersionOperator(BaseOperator):
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
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        dataset_id: str,
        datasource_id: str,
        credential_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.datasource_id = datasource_id
        self.credential_id = credential_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

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


class CreateOrUpdateDataSourceOperator(BaseOperator):
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
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        data_store_id: str,
        datarobot_conn_id: str = "datarobot_default",
        dataset_name: Optional[str] = "{{ params.get('dataset_name', '') }}",
        table_schema: Optional[str] = "{{ params.get('table_schema', '') }}",
        table_name: Optional[str] = "{{ params.get('table_name', '') }}",
        query: Optional[str] = "{{ params.get('query', '') }}",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.data_store_id = data_store_id
        self.datarobot_conn_id = datarobot_conn_id
        self.dataset_name = dataset_name
        self.table_schema = table_schema
        self.table_name = table_name
        self.query = query

        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> Optional[str]:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        self.log.debug(f"Trying to get existing DataStore by data_store_id={self.data_store_id}")
        data_store = dr.DataStore.get(data_store_id=self.data_store_id)
        self.log.debug(f"Found existing DataStore: {data_store.canonical_name}, id={data_store.id}")

        if not self.dataset_name:
            self.dataset_name = self._get_default_data_source_name(data_store.id)
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

    def _get_default_data_source_name(self, data_store_id) -> str:
        """Build default name based on the data source params."""
        parts = ["Airflow", data_store_id]

        if self.query:
            parts += ["q", sha256(self.query.encode()).hexdigest()]

        else:
            parts += ["t", self.table_schema, self.table_name]

        return "-".join(parts)


class CreateWranglingRecipeOperator(BaseOperator):
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
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        datarobot_conn_id: str = "datarobot_default",
        use_case_id: str = '{{ params.get("use_case_id", "") }}',
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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        self.use_case_id = use_case_id
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

        if kwargs.get("xcom_push") is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Context) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if not self.use_case_id:
            raise AirflowException(
                "*use_case_id* is a mandatory parameter. "
                "You can set it either explicitly or via the context variable *use_case_id*"
            )

        if self.dataset_id and self.data_store_id:
            raise AirflowException(
                "You have to specify either dataset_id or data_store_id. Not both."
            )

        use_case = dr.UseCase.get(self.use_case_id)

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
                    )
                ],
            )

        else:
            raise AirflowException("Specify either dataset_id or data_store_id to wrangle.")

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
