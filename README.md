# DataRobot Provider for Apache Airflow
[![Documentation](https://img.shields.io/badge/docs-readthedocs-forestgreen)](https://datarobot-datarobot-airflow-provider.readthedocs-hosted.com/en/latest/)
[![PyPI version](https://img.shields.io/pypi/v/airflow-provider-datarobot)](https://pypi.org/project/airflow-provider-datarobot/)
![Python versions](https://img.shields.io/pypi/pyversions/airflow-provider-datarobot)
![License](https://img.shields.io/pypi/l/airflow-provider-datarobot)

This package provides operators, sensors, and a hook to integrate [DataRobot](https://www.datarobot.com) into Apache Airflow.
Using these components, you should be able to build the essential DataRobot pipeline - create a project, train models, deploy a model,
and score predictions against the model deployment.

## Install the Airflow provider

**To run Airflow within DataRobot SaaS environment, please reach out to [DataRobot Support](https://www.datarobot.com/contact-us/).**

For a local installation, the DataRobot provider for Apache Airflow requires an environment with the following dependencies installed:

* [Apache Airflow](https://pypi.org/project/apache-airflow/) >= 2.3

* [DataRobot Python API Client](https://pypi.org/project/datarobot/) >= 3.2.0

To install the DataRobot provider, run the following command:

``` sh
pip install airflow-provider-datarobot
```

## Create a connection from Airflow to DataRobot

The next step is to create a connection from Airflow to DataRobot:

1. In the Airflow user interface, click **Admin > Connections** to
   [add an Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui).

2. On the **List Connection** page, click **+ Add a new record**.

3. In the **Add Connection** dialog box, configure the following fields:

    | Field          | Description |
    |----------------|-------------|
    |Connection Id   | `datarobot_default` (this name is used by default in all operators) |
    |Connection Type | DataRobot |
    |API Key         | A DataRobot API key, created in the [DataRobot Developer Tools](https://app.datarobot.com/account/developer-tools), from the [*API Keys* section](https://app.datarobot.com/docs/api/api-quickstart/api-qs.html#create-a-datarobot-api-token). |
    |DataRobot endpoint URL | `https://app.datarobot.com/api/v2` by default |

4. Click **Test** to establish a test connection between Airflow and DataRobot.

5. When the connection test is successful, click **Save**.

## JSON configuration for the DAG run

Operators and sensors use parameters from the [config](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html?highlight=config#Named%20Arguments_repeat21) JSON submitted when triggering the DAG; for example:


``` yaml
{
    "training_data": "s3-presigned-url-or-local-path-to-training-data",
    "project_name": "Project created from Airflow",
    "autopilot_settings": {
        "target": "readmitted"
    },
    "deployment_label": "Deployment created from Airflow",
    "score_settings": {
        "intake_settings": {
            "type": "s3",
            "url": "s3://path/to/scoring-data/Diabetes10k.csv",
            "credential_id": "<credential_id>"
        },
        "output_settings": {
            "type": "s3",
            "url": "s3://path/to/results-dir/Diabetes10k_predictions.csv",
            "credential_id": "<credential_id>"
        }
    }
}
```


These config values are accessible in the `execute()` method of any operator in the DAG
through the `context["params"]` variable; for example, to get training data, you could use the following:

``` py
def execute(self, context: Context) -> str:
    ...
    training_data = context["params"]["training_data"]
    ...
```

## Development
### Pre-requisites
- [Docker Desktop 1.13.1 or later](https://docs.docker.com/desktop/)
- [Astronomer CLI 1.30.0 or later](https://github.com/astronomer/astro-cli?tab=readme-ov-file#install-the-astro-cli)
- [Pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation) or [virtualenv](https://virtualenv.pypa.io/en/latest/)

### Environment Setup
It is useful to have a simple airflow testing environment and a local development environment for the
operators and DAGs. The following steps will construct the two environments needed for development.
1. Clone the `airflow-provider-datarobot` repository
    ```bash
        cd ~/workspace
        git clone git@github.com:datarobot/airflow-provider-datarobot.git
        cd airflow-provider-datarobot
    ```
2. Create a virtual environment and install the dependencies
    ```bash
        pyenv virtualenv 3.12 airflow-provider-datarobot
        pyenv local airflow-provider-datarobot
        make req-dev
        pre-commit install
    ```

### Astro Setup
1. (OPTIONAL) Install astro with the following command or manually from the links above:
    ```bash
        make install-astro
    ```
2. Build an astro development environment with the following command:
    ```bash
        make create-astro-dev
    ```
3. A new `./astro-dev` folder will be constructed for you to use as a development and test environment.
4. Compile and run airflow on the development package with:
    ```bash
        make build-astro-dev
    ```

_Note: All credentials and logins will be printed in the terminal after running
the `build-astro-dev` command._

### Updating Operators in the Dev Environment
- Test, compile, and run new or updated operators on the development package with:
    ```bash
        make build-astro-dev
    ```
- Manually start the airflow dev environment without rebuilding the package with:
    ```bash
        make start-astro-dev
    ```
- Manually stop the airflow dev environment without rebuilding the package with:
    ```bash
        make stop-astro-dev
    ```
- If there are problems with the airflow environment you can reset it to a clean state with:
    ```bash
        make clean-astro-dev
    ```


### Release Process
For `mainline` releases, the following steps should be followed:
- Determine the next version of the package (example: 1.0.2). Version should not include a `v` prefix.
- Determine the SHA hash of the commit that will be the release.
  - See: https://github.com/datarobot/airflow-provider-datarobot/commits/main/
- Connect to `harness`.
- Run the `create-release-pr` pipeline with the SHA hash and version as parameters.
- Review and approve the release PR on GitHub.
  - Changes or comments can be added to the PR.
  - The PR will automatically request review once checks pass.
- Merge the PR and use the resulting SHA hash from merge to main in the next step (different SHA from previous step)
- Run the `create-release-tag` pipeline with the SHA hash and version as parameters.
- Run the `release-pypi` pipeline with the input set as `Git Tag` and the `Tag Name` as the version (tags are generated with a `v` prefix, example v1.0.2).

For `early-access` releases, run the `release-early-access-pypi` pipeline. There are no PRs or tags for early-access releases. The early access version is also automatically released each Tuesday.


## Issues

Please submit [issues](https://github.com/datarobot/airflow-provider-datarobot/issues) and [pull requests](https://github.com/datarobot/airflow-provider-datarobot/pulls) in our official repo:
[https://github.com/datarobot/airflow-provider-datarobot](https://github.com/datarobot/airflow-provider-datarobot)

We are happy to hear from you. Please email any feedback to the authors at [support@datarobot.com](mailto:support@datarobot.com).


# Copyright Notice

Copyright 2023 DataRobot, Inc. and its affiliates.

All rights reserved.

This is proprietary source code of DataRobot, Inc. and its affiliates.

Released under the terms of DataRobot Tool and Utility Agreement.
