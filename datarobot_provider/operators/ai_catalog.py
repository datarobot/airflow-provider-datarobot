# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
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
        credential, credential_data, data_store = JDBCDataSourceHook(
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
    Creates the data source or updates it if its already exist and return data source ID.

    :param data_store_id: DataRobot data store ID
    :type data_store_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog data source ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["data_store_id"]
    template_fields_renderers: dict[str, str] = {}
    template_ext: Sequence[str] = ()
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        data_store_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.data_store_id = data_store_id
        self.datarobot_conn_id = datarobot_conn_id
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

        dataset_name = context["params"]["dataset_name"]

        # Creating DataSourceParameters:
        if "query" in context["params"] and context["params"]["query"]:
            # using sql statement if provided:
            params = dr.DataSourceParameters(query=context["params"]["query"])
        else:
            # otherwise using schema and table:
            params = dr.DataSourceParameters(
                schema=context["params"]["table_schema"], table=context["params"]["table_name"]
            )

        self.log.debug(f"Trying to get existing DataSource by name={dataset_name}")
        for dr_source_item in dr.DataSource.list():
            if dr_source_item.canonical_name == dataset_name:
                data_source = dr_source_item
                self.log.info(f"Found existing DataSource:{dataset_name}, id={data_source.id}")
                # Checking if there are any changes in params:
                if params != data_source.params:
                    # If params in changed, updating data source:
                    self.log.info(f"Updating DataSource:{dataset_name} with new params")
                    data_source.update(canonical_name=dataset_name, params=params)
                    self.log.info(
                        f"DataSource:{dataset_name} successfully updated, id={data_source.id}"
                    )
                break
        else:
            # Adding data_store_id to params (required for DataSource creation):
            params.data_store_id = data_store.id
            # Creating DataSource using params with data_store_id
            self.log.info(f"Creating DataSource: {dataset_name}")
            data_source = dr.DataSource.create(
                data_source_type="jdbc", canonical_name=dataset_name, params=params
            )
            self.log.info(f"DataSource:{dataset_name} successfully created, id={data_source.id}")

        return data_source.id
