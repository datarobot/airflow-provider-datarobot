# Copyright 2022 DataRobot, Inc. and its affiliates.
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

DEFAULT_MAX_WAIT_SEC = 600


class AddExternalDatasetOperator(BaseOperator):
    """
    Upload a new dataset from a catalog dataset to make predictions for a model
    :param project_id: DataRobot project ID
    :type project_id: str
    :param dataset_id: DataRobot AI Catalog dataset ID
    :type dataset_id: str
    :param credential_id: DataRobot Credentials ID
    :type credential_id: str
    :param dataset_version_id: DataRobot AI Catalog dataset version ID
    :type dataset_version_id: str
    :param datarobot_conn_id: Connection ID, defaults to `datarobot_default`
    :type datarobot_conn_id: str, optional
    :return: Feature Impact job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "project_id",
        "dataset_id",
        "credential_id",
        "dataset_version_id",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        project_id: str = None,
        dataset_id: str = None,
        credential_id: str = None,
        dataset_version_id: str = None,
        max_wait_sec: int = DEFAULT_MAX_WAIT_SEC,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credential_id = credential_id
        self.dataset_version_id = dataset_version_id
        self.max_wait_sec = max_wait_sec
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.project_id is None:
            raise ValueError("project_id is required to add external dataset.")

        if self.dataset_id is None:
            raise ValueError("dataset_id is required to add external dataset.")

        project = dr.Project.get(self.project_id)

        external_dataset = project.upload_dataset_from_catalog(
            dataset_id=self.dataset_id,
            credential_id=self.credential_id,
            dataset_version_id=self.dataset_version_id,
            max_wait=self.max_wait_sec,
        )

        self.log.info(
            f"External Dataset added to the Project, external dataset_id={external_dataset.id}"
        )

        return external_dataset.id
