stage('Build and Publish'){
    node('ubuntu:focal && 2xCPU~4xRAM'){

    String testpypi_repo_jenkins_creds_path = "jenkins/mlops/airflow_provider_datarobot/test-pypi"

    checkout scm
        withCredentials([
        usernamePassword(
            credentialsId: testpypi_repo_jenkins_creds_path,
            passwordVariable: 'TWINE_PASSWORD',
            usernameVariable: 'TWINE_USERNAME'
        ),
        ])
            {
            sh """
              set -e
              virtualenv .venv -p python3.8
              source .venv/bin/activate
              pip install --upgrade pip wheel setuptools
              pip install -r requirements.txt
              pip install --upgrade build twine
              echo "Building wheel..."
              python -m build --no-isolation

              echo "Publishing to PyPi (${env.PUBLISH_REPO_URL})..."
              twine upload dist/*.whl \
                --repository-url "${env.PUBLISH_REPO_URL}" \
                --username "$TWINE_USERNAME" \
                --password "$TWINE_PASSWORD" \
                --non-interactive \
                --disable-progress-bar
              echo "Finished successfully!"
            """
        }
    }
}
