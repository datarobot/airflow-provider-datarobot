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
from datetime import datetime

from setuptools import find_packages
from setuptools import setup

from setup_common import DESCRIPTION_TEMPLATE
from setup_common import common_setup_kwargs
from setup_common import version

version += datetime.today().strftime(".%Y.%m.%d")

python_versions = ">= 3.9"

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Topic :: Scientific/Engineering :: Artificial Intelligence",
    "License :: Other/Proprietary License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
]

description = DESCRIPTION_TEMPLATE.format(
    package_name="airflow_provider_datarobot_early_access",
    pypi_url_target="https://pypi.python.org/pypi/airflow-provider-datarobot-early-access/",
    extra_desc=(
        'This package is the "early access" version of the client. **Do NOT use this package'
        " in production--you will expose yourself to risk of breaking changes and bugs.** For"
        " the most stable version, see https://pypi.org/project/airflow-provider-datarobot/."
    ),
    python_versions=python_versions,
    pip_package_name="airflow_provider_datarobot_early_access",
)

packages = find_packages(exclude=["tests*"])

common_setup_kwargs.update(
    name="airflow_provider_datarobot_early_access",
    version=version,
    packages=packages,
    long_description=description,
    classifiers=classifiers,
)

setup(**common_setup_kwargs)
