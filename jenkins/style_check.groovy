stage('Style Check') {
    node('ubuntu:focal && 2xCPU~2xRAM') {
        checkout scm
            sh """
              #!/bin/bash
              set -xe
              virtualenv .venv -p python3.8
              source .venv/bin/activate
              pip install -r requirements.txt
              make lint check-licenses
            """
            // TODO: need to run black and isort checks
    }
}
