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

from setup_common import DESCRIPTION_TEMPLATE
from setup_common import classifiers
from setup_common import common_setup_kwargs
from setup_common import version

if "b" in version:
    msg = (
        "Major releases must not have a 'b' for beta in the version. "
        "Go back and make a release branch with tag, then update the version number."
    )
    raise RuntimeError(msg)

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
