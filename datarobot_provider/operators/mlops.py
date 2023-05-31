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
from typing import List

import datarobot as dr
import datarobot.client as dr_client
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook

DATAROBOT_MAX_WAIT = 3600
DATAROBOT_AUTOPILOT_TIMEOUT = 86400
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%s"


class UploadActualsOperator(BaseOperator):

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = ["deployment_id", "dataset_id", "dataset_version_id", "credential_id"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        deployment_id: str = None,
        dataset_id: str = None,
        dataset_version_id: str = None,
        credential_id: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.deployment_id = deployment_id
        self.dataset_id = dataset_id
        self.dataset_version_id = dataset_version_id
        self.datarobot_conn_id = datarobot_conn_id
        self.credential_id = credential_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        client = DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        # Uploading Actuals from AI Catalog
        self.log.info("Uploading Actuals from AI Catalog")

        actuals_config = {
            'actualValueColumn': 'ACTUAL',
            'associationIdColumn': 'id',
            'datasetId': self.dataset_id,
            # 'datasetVersionId' : dataset.version_id,
            # 'timestampColumn' : '<INSERT COLUMN NAME>',
            # 'wasActedOnColumn' : '<INSERT COLUMN NAME>'
        }

        res = client.post(f'deployments/{self.deployment_id}/actuals/fromDataset', data=actuals_config)

        async_status_check = res.headers['Location']
        print(async_status_check)
        if True:
            return async_status_check

        else:
            raise AirflowFailException(
                'Error Message here'
            )
