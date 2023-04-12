stage('build and publish'){
    node('ubuntu:focal && 2xCPU~4xRAM'){

    String pypi_repo_url = "https://test.pypi.org/legacy/"

    String notify_channel = "external-agents-911"
    String build_info_msg = "Open this build on Jenkins: ${env.BUILD_URL}"

    checkout scm
            //sh 'bash jenkins/python_scripts/publish_python_package.sh ' + pypi_repo_url
            sh """
              #!/bin/bash
              set -xe
              virtualenv .venv -p python3.8
              source .venv/bin/activate
              pip install -r requirements.txt
              echo "Show Airflow version:"
              echo airflow version
              echo "Show DataRobot Client version:"
              pip show datarobot
              pip install --upgrade build
              echo "Building wheel..."
              python -m build
            """
    }
}