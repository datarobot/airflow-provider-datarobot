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

from setup import DESCRIPTION_TEMPLATE
from setup import common_setup_kwargs
from setup import version

version += datetime.today().strftime(".%Y.%m.%d")

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
]

# RELEASE SETUP
package_name = "airflow_provider_datarobot_early_access"
pip_package_name = "airflow-provider-datarobot-early-access"

description = DESCRIPTION_TEMPLATE.format(
    package_name=package_name,
    pypi_url_target="https://pypi.python.org/pypi/airflow-provider-datarobot-early-access/",
    extra_desc=(
        'This package is the "early access" version of the client. **Do NOT use this package'
        " in production--you will expose yourself to risk of breaking changes and bugs.** For"
        " the most stable version, see https://pypi.org/project/airflow-provider-datarobot/."
    ),
    pip_package_name=pip_package_name,
)

packages = find_packages(exclude=["docs*", "tests*"])

common_setup_kwargs.update(
    name=package_name,
    version=version,
    packages=packages,
    long_description=description,
    classifiers=classifiers,
    install_requires=["apache-airflow>=2.6.0", "datarobot-early-access"],
)

setup(**common_setup_kwargs)
