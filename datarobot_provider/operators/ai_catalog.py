# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from typing import Any
from typing import Dict
from typing import Iterable

import datarobot as dr
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook

DATAROBOT_MAX_WAIT = 3600
DATAROBOT_AUTOPILOT_TIMEOUT = 86400
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%s"


class UploadDatasetOperator(BaseOperator):
    """
    Uploading local file to DataRobot AI Catalog and return Dataset ID.

    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = []
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Upload Dataset to AI Catalog
        self.log.info("Upload Dataset to AI Catalog")
        # dataset_file_path a path to a local file
        ai_catalog_dataset = dr.Dataset.create_from_file(context["params"]["dataset_file_path"])
        self.log.info(f"Dataset created: dataset_id={ai_catalog_dataset.id}")
        return ai_catalog_dataset.id


class UpdateDatasetFromFileOperator(BaseOperator):
    """
    Operator that creates a new Dataset version from a file.
    Returns when the new dataset version has been successfully uploaded.

    :param dataset_id: DataRobot AI Catalog dataset ID
    :type dataset_id: str, optional
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: DataRobot AI Catalog dataset version ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = ["dataset_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        dataset_id: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # If dataset_id not provided in constructor, then using training_dataset_id from json config
        dataset_id = (
            self.dataset_id
            if self.dataset_id is not None
            else context['params']['training_dataset_id']
        )

        # The path to the file.
        file_path = context["params"]["dataset_file_path"]
        self.log.info(f"Update Dataset {dataset_id} in AI Catalog from the local file: {file_path}")
        ai_catalog_dataset = dr.Dataset.create_version_from_file(
            dataset_id=dataset_id, file_path=file_path
        )
        self.log.info(
            f"Dataset updated: dataset_id={ai_catalog_dataset.id}, dataset_version_id:{ai_catalog_dataset.version_id}"
        )

        return ai_catalog_dataset.version_id
