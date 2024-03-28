# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import json
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable

import datarobot as dr
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import BaseOperator

from datarobot_provider.hooks.datarobot import DataRobotHook


class CreateCustomJobOperator(BaseOperator):
    """
    Create a DataRobot custom job.
    :param name: custom job name
    :type name: str
    :param description: custom job description
    :type description: str, optional
    :return: custom job ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "name",
        "description",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        name: str,
        description: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.name is None:
            raise ValueError("Invalid or missing custom job name")

        response = dr.client.get_client().post(
            "customJobs/",
            data={
                "name": self.name,
                "description": self.description,
            },
        )

        if response.status_code == 201:
            custom_job_id = response.json()["id"]
            self.log.info(f"Custom job created, custom_job_id={custom_job_id}")
            return custom_job_id
        else:
            e_msg = "Server unexpectedly returned status code {}"
            raise AirflowFailException(e_msg.format(response.status_code))


class AddFilesToCustomJobOperator(BaseOperator):
    """
    Adding files to custom job from specified location.
    :param custom_job_id: custom job ID
    :type custom_job_id: str
    :param files_path: files location to add
    :type files_path: str
    :return: list of files added to the custom job
    :rtype: List[str]
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "custom_job_id",
        "files_path",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        custom_job_id: str = None,
        files_path: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_job_id = custom_job_id
        self.files_path = files_path
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def upload_custom_job_file(self, file_path, filename):
        with open(file_path, "rb") as file_payload:
            files = {
                'file': file_payload,
                'filePath': filename,
            }

            response = dr.client.get_client().build_request_with_file(
                form_data=files,
                fname=filename,
                filelike=file_payload,
                url=f"customJobs/{self.custom_job_id}/",
                method="patch",
            )

        return response.status_code

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        airflow_home = os.environ.get('AIRFLOW_HOME')
        self.files_path = Path(f'{airflow_home}/dags/{self.files_path}')
        if self.custom_job_id is None:
            raise ValueError("Invalid or missing custom_job_id")

        if self.files_path is None:
            raise ValueError("Invalid or missing files_path")
        uploaded_files = []
        if os.path.isdir(self.files_path):
            for file_name in os.listdir(self.files_path):
                self.upload_custom_job_file(self.files_path / file_name, file_name)
                uploaded_files.append(file_name)
        return uploaded_files


class SetCustomJobExecutionEnvironmentOperator(BaseOperator):
    """
    Set execution environment to the custom job.
    :param custom_job_id: DataRobot custom job ID
    :type custom_job_id: str
    :param environment_id: execution environment ID
    :type environment_id: str
    :param environment_version_id: execution environment version ID
    :type environment_version_id: str
    :return: operation response
    :rtype: dict
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "custom_job_id",
        "environment_id",
        "environment_version_id",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        custom_job_id: str,
        environment_id: str,
        environment_version_id: str,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_job_id = custom_job_id
        self.environment_id = environment_id
        self.environment_version_id = environment_version_id
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.custom_job_id is None:
            raise ValueError("Invalid or missing custom_job_id")

        if self.environment_id is None:
            raise ValueError("Invalid or missing environment_id")

        if self.environment_version_id is None:
            raise ValueError("Invalid or missing environment_version_id")

        form_data = {
            "environmentId": self.environment_id,
            "environmentVersionId": self.environment_version_id,
        }

        response = dr.client.get_client().patch(
            url=f"customJobs/{self.custom_job_id}/",
            data=form_data,
        )

        if response.status_code == 201:
            response_json = response.json()
            custom_job_id = response_json["id"]
            environment_id = response_json["environmentId"]
            environment_version_id = response_json["environmentVersionId"]
            self.log.info(
                f"Custom job_id={custom_job_id} environment updated "
                f"with environment_id={environment_id} "
                f"and environment_version_id={environment_version_id}"
            )
            return response_json
        else:
            e_msg = "Server unexpectedly returned status code {}"
            raise AirflowFailException(e_msg.format(response.status_code))


class SetCustomJobRuntimeParametersOperator(BaseOperator):
    """
    Create an execution environment.
    :param name: execution environment name
    :type name: str
    :param description: execution environment description
    :type description: str, optional
    :param programming_language: programming language of the environment to be created.
        Can be "python", "r", "java" or "other". Default value - "other"
    :type programming_language: str, optional
    :return: execution environment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = ["custom_job_id", "runtime_parameter_values"]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        custom_job_id: str,
        runtime_parameter_values: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_job_id = custom_job_id
        self.runtime_parameter_values = runtime_parameter_values
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.runtime_parameter_values is None:
            raise ValueError("Invalid or missing runtime_parameter_values")

        if isinstance(self.runtime_parameter_values, list):
            self.runtime_parameter_values = json.dumps(self.runtime_parameter_values)

        form_data = {
            "runtimeParameterValues": self.runtime_parameter_values,
        }

        response = dr.client.get_client().patch(
            url=f"customJobs/{self.custom_job_id}/",
            data=form_data,
        )

        if response.status_code == 201:
            response_json = response.json()
            custom_job_id = response_json["id"]
            self.log.info(f"Custom job_id={custom_job_id} environment updated")
            return response_json
        else:
            e_msg = "Server unexpectedly returned status code {}"
            raise AirflowFailException(e_msg.format(response.status_code))


class RunCustomJobOperator(BaseOperator):
    """
    Run custom job and return ID for job status check.
    :param custom_job_id: custom job ID
    :type custom_job_id: str
    :param setup_dependencies: setup dependencies flag
    :type bool, optional
    :return: status check ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "custom_job_id",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        custom_job_id: str = None,
        setup_dependencies: bool = False,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.custom_job_id = custom_job_id
        self.setup_dependencies = setup_dependencies
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.custom_job_id is None:
            raise ValueError("Invalid or missing custom job id")

        response = dr.client.get_client().post(
            f"customJobs/{self.custom_job_id}/runs/",
            data={"setupDependencies": self.setup_dependencies},
        )

        if response.status_code == 201:
            response_json = response.json()
            job_status_id = response_json["jobStatusId"]
            custom_job_id = response.json()["id"]
            self.log.info(f"Custom job created, custom_job_id={custom_job_id}")
            return job_status_id
        else:
            e_msg = "Server unexpectedly returned status code {}"
            raise AirflowFailException(e_msg.format(response.status_code))


class ListExecutionEnvironmentOperator(BaseOperator):
    """
    List all exising execution environments that matches search condition.
    :param search_for: the string for filtering execution environment - only execution
            environments that contain the string in name or description will
            be returned.
    :type search_for: str, optional
    :return: execution environment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "search_for",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        search_for: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.search_for = search_for
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        execution_environments = dr.ExecutionEnvironment.list(search_for=self.search_for)
        execution_environment_ids = [
            execution_environment.id for execution_environment in execution_environments
        ]
        self.log.info(f"List of execution environments ids = {execution_environment_ids}")

        return execution_environment_ids


class ListExecutionEnvironmentVersionsOperator(BaseOperator):
    """
    List all exising execution environments versions that matches search condition.
    :param search_for: the string for filtering execution environment - only execution
            environments that contain the string in name or description will
            be returned.
    :type search_for: str, optional
    :return: execution environment ID
    :rtype: str
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Iterable[str] = [
        "environment_id",
        "search_for",
    ]
    template_fields_renderers: Dict[str, str] = {}
    template_ext: Iterable[str] = ()
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        environment_id: str,
        search_for: str = None,
        datarobot_conn_id: str = "datarobot_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.environment_id = environment_id
        self.search_for = search_for
        self.datarobot_conn_id = datarobot_conn_id
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )

    def execute(self, context: Dict[str, Any]) -> str:
        # Initialize DataRobot client
        DataRobotHook(datarobot_conn_id=self.datarobot_conn_id).run()

        if self.environment_id is None:
            raise ValueError("Invalid or missing environment_id")

        execution_environments = dr.ExecutionEnvironmentVersion.list(
            execution_environment_id=self.environment_id,
            # search_for=self.search_for
        )
        execution_environment_ids = [
            execution_environment.id for execution_environment in execution_environments
        ]
        self.log.info(f"List of execution environments ids = {execution_environment_ids}")

        return execution_environment_ids
