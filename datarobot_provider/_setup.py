import re

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
        "Documentation": "https://github.com/datarobot/airflow-provider-datarobot/",
        "Changelog": "https://github.com/datarobot/airflow-provider-datarobot/blob/main/CHANGES.md",
    },
    license="DataRobot Tool and Utility Agreement",
    packages=None,
    package_data={"airflow_provider_datarobot": ["py.typed"]},
    python_requires=">=3.9",
    long_description=None,
    long_description_content_type="text/markdown",
    classifiers=None,
    install_requires=["apache-airflow>=2.3.0", "datarobot>=3.6.1"],
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
            "Sphinx>=8.1.3",
            "sphinx_rtd_theme>=3.0",
            "sphinx-autodoc-typehints>=2",
            "pyenchant==3.2.2",
            "sphinx-copybutton",
            "sphinx-markdown-builder",
            "myst-parser==4.0.0",
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
    "Programming Language :: Python :: 3.13",
]

DESCRIPTION_TEMPLATE = """
About {package_name}
=============================================
.. image:: https://img.shields.io/pypi/v/{package_name}.svg
   :target: {pypi_url_target}
.. image:: https://img.shields.io/pypi/pyversions/{package_name}.svg
.. image:: https://img.shields.io/pypi/status/{package_name}.svg

This package provides operators, sensors, and a hook to integrate [DataRobot](https://www.datarobot.com)
into Apache Airflow. {extra_desc}

This package is released under the terms of the DataRobot Tool and Utility Agreement, which
can be found on our `Legal`_ page, along with our privacy policy and more.

Installation
=========================
The DataRobot provider for Apache Airflow requires an environment with the following dependencies installed:

You must have a datarobot account.

::

   $ pip install {pip_package_name}

Bug Reporting and Q&A
=========================
To report issues or ask questions, send email to `the team <api-maintainer@datarobot.com>`_.

.. _datarobot: https://datarobot.com
.. _documentation: https://github.com/datarobot/airflow-provider-datarobot
.. _legal: https://www.datarobot.com/legal/
"""

with open("datarobot_provider/_version.py") as fd:
    version_search = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE)
    if not version_search:
        raise RuntimeError("Cannot find version information")
    version = version_search.group(1)

if not version:
    raise RuntimeError("Cannot find version information")
