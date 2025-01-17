# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()


dev_require = [
    "pytest>=7.0.0",
    "pytest-mock>=3.7.0",
    "pytest-helpers-namespace>=2021.12.29",
    "Sphinx>=8.1.3 ; python_version >= '3.11'",
    "sphinx_rtd_theme>=3.0",
    "numpydoc>=1.7.0,<1.8.0",
    "sphinx-autodoc-typehints>=2 ; python_version >= '3.8'",
    "pyenchant==3.2.2",
    "sphinx-copybutton",
    "sphinx-markdown-builder",
    "myst-parser==4.0.0",
]

"""Perform the package airflow-provider-datarobot setup."""
setup(
    name="airflow-provider-datarobot",
    version="0.0.10",
    description="DataRobot Airflow provider.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": ["provider_info=datarobot_provider.__init__:get_provider_info"]
    },
    license="DataRobot Tool and Utility Agreement",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: Other/Proprietary License",
    ],
    packages=[
        "datarobot_provider",
        "datarobot_provider.hooks",
        "datarobot_provider.sensors",
        "datarobot_provider.operators",
    ],
    install_requires=[
        'apache-airflow>=2.3.0',
        'datarobot>=3.6.0'
    ],
    setup_requires=['setuptools', 'wheel'],
    author='DataRobot',
    author_email='support@datarobot.com',
    url='https://www.datarobot.com/',
    python_requires='~=3.7',
    extras_require={
        "dev": dev_require,
    }
)
