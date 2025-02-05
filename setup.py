#
# Copyright 2021-2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# DataRobot, Inc.
#
# This is proprietary source code of DataRobot, Inc. and its
# affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from setuptools import find_packages
from setuptools import setup

from datarobot_provider._setup import DESCRIPTION_TEMPLATE
from datarobot_provider._setup import classifiers
from datarobot_provider._setup import common_setup_kwargs
from datarobot_provider._setup import version

python_versions = ">= 3.9"

description = DESCRIPTION_TEMPLATE.format(
    package_name="airflow_provider_datarobot",
    pypi_url_target="https://pypi.python.org/pypi/airflow-provider-datarobot/",
    extra_desc="",
    python_versions=python_versions,
    pip_package_name="airflow_provider_datarobot",
)

packages = find_packages(exclude=["tests*"])

common_setup_kwargs.update(
    name="airflow_provider_datarobot",
    version=version,
    packages=packages,
    long_description=description,
    classifiers=classifiers,
)

setup(**common_setup_kwargs)
