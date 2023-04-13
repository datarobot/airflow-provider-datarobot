stage('Build and Publish'){
    node('ubuntu:focal && 2xCPU~4xRAM'){

    String pypi_repo_url = "https://test.pypi.org/legacy/"

    String notify_channel = "external-agents-911"
    String build_info_msg = "Open this build on Jenkins: ${env.BUILD_URL}"

    checkout scm
            sh """
              #!/bin/bash
              set -xe
              virtualenv .venv -p python3.8
              source .venv/bin/activate
              pip install -r requirements.txt
              echo "Show Airflow version:"
              airflow version
              echo "Show DataRobot Client version:"
              pip show datarobot
              pip install --upgrade build
              echo "Building wheel..."
              python -m build --no-isolation
              echo "Install twine tool..."
              pip install --upgrade pip build twine
              twine upload --repository testpypi dist/airflow_provider_datarobot-0.0.5-py3-none-any.whl
            """
    }
}