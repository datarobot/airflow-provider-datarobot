stage('Unit Test') {
    node('ubuntu:focal && 2xCPU~2xRAM') {
        checkout scm
        try {
            sh """
              #!/bin/bash
              set -xe
              virtualenv .venv -p python3.8
              source .venv/bin/activate
              pip install apache-airflow==2.5.1
              pip install -r requirements.txt
              pip install airflow-provider-datarobot
              airflow db init
              airflow db check
              pytest -vv tests/unit/ --junit-xml=unit_test_report.xml
            """
        } finally {
            junit(testResults: "unit_test_report.xml", skipPublishingChecks: true)
        }
    }
}