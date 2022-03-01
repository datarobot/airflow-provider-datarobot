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

"""Perform the package airflow-provider-datarobot setup."""
setup(
    name='airflow-provider-datarobot',
    version="0.0.1",
    description='DataRobot Airflow provider.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points={
        "apache_airflow_provider": [
            "provider_info=datarobot_provider.__init__:get_provider_info"
        ]
    },
    license='Apache License 2.0',
    packages=['datarobot_provider', 'datarobot_provider.hooks',
              'datarobot_provider.sensors', 'datarobot_provider.operators'],
    install_requires=['apache-airflow>=2.0'],
    setup_requires=['setuptools', 'wheel'],
    author='Andrius Senulis',
    author_email='andrius.senulis@datarobot.com',
    url='http://www.datarobot.com/',
    python_requires='~=3.7',
)
