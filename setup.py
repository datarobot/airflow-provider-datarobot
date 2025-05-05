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
import re

from setuptools import find_packages
from setuptools import setup

common_setup_kwargs = dict(
    name=None,
    version=None,
    description="This client library is designed to integrate Airflow with the DataRobot API.",
    author="datarobot",
    author_email="api-maintainer@datarobot.com",
    maintainer="datarobot",
    maintainer_email="api-maintainer@datarobot.com",
    url="https://datarobot.com",
    project_urls={
        "Documentation": "https://datarobot-datarobot-airflow-provider.readthedocs-hosted.com/en/latest/",
        "Changelog": "https://github.com/datarobot/airflow-provider-datarobot/blob/main/CHANGES.md",
        "Bug Tracker": "https://github.com/datarobot/airflow-provider-datarobot/issues",
        "Source Code": "https://github.com/datarobot/airflow-provider-datarobot",
    },
    license="DataRobot Tool and Utility Agreement",
    packages=None,
    package_data={"airflow_provider_datarobot": ["py.typed"]},
    python_requires=">=3.9",
    long_description=None,
    long_description_content_type="text/markdown",
    classifiers=None,
    install_requires=["apache-airflow>=2.6.0,<3.0", "datarobot>=3.7.0"],
    entry_points={
        "apache_airflow_provider": [
            "provider_info = datarobot_provider.__init__:get_provider_info",
        ],
    },
    extras_require={
        "dev": [
            "pre-commit>=4.0.1",
            "ruff>=0.9.2",
            "mypy>=0.931",
            "pytest>=7.0.0",
            "pytest-mock>=3.7.0",
            "pytest-helpers-namespace>=2021.12.29",
            "numpydoc>=1.7.0,<1.8.0",
            "black==24.10.0",
            "pyyaml>=6.0.2",
            "types-PyYAML>=6.0.12",
            "freezegun>=1.5.1",
        ],
        "docs": [
            "Sphinx==8.1.3",
            "sphinx_rtd_theme>=3.0",
            "sphinx-autodoc-typehints>=2",
            "pyenchant==3.2.2",
            "sphinx-copybutton",
            "sphinx-markdown-builder",
            "myst-parser==4.0.0",
            "numpydoc>=1.7.0,<1.8.0",
        ],
    },
)

classifiers = [
    "Development Status :: 5 - Production/Stable",
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

DESCRIPTION_TEMPLATE = """
About {package_name}
=============================================
![package](https://img.shields.io/pypi/v/{package_name}.svg)
![versions](https://img.shields.io/pypi/pyversions/{package_name}.svg)
![status](https://img.shields.io/pypi/status/{package_name}.svg)

This package provides operators, sensors, and a hook to integrate [DataRobot](https://www.datarobot.com)
into Apache Airflow. {extra_desc}

This package is released under the terms of the DataRobot Tool and Utility Agreement, which
can be found on our [Legal](https://www.datarobot.com/legal/) page, along with our privacy policy and more.

Installation
=========================
You must have a datarobot account.

```
$ pip install {pip_package_name}
```

Bug Reporting and Q&A
=========================
To report issues or ask questions, send email to [the team](mailto:api-maintainer@datarobot.com).
"""

with open("datarobot_provider/_version.py") as fd:
    version_search = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE)
    if not version_search:
        raise RuntimeError("Cannot find version information")
    version = version_search.group(1)

if not version:
    raise RuntimeError("Cannot find version information")

# RELEASE SETUP
package_name = "airflow_provider_datarobot"

description = DESCRIPTION_TEMPLATE.format(
    package_name=package_name,
    pypi_url_target="https://pypi.python.org/pypi/airflow-provider-datarobot/",
    extra_desc="",
    pip_package_name=package_name,
)

packages = find_packages(exclude=["docs*", "tests*", "*_experimental*"])

common_setup_kwargs.update(
    name=package_name,
    version=version,
    packages=packages,
    long_description=description,
    classifiers=classifiers,
)

setup(**common_setup_kwargs)
