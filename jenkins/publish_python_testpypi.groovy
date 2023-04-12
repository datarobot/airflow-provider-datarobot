node('ubuntu:focal'){
    String pypi_repo_url = "https://test.pypi.org/legacy/"

    String notify_channel = "external-agents-911"
    String build_info_msg = "Open this build on Jenkins: ${env.BUILD_URL}"

    checkout scm

    try{
        stage('build and publish'){
                sh 'bash jenkins/python_scripts/publish_python_package.sh ' + pypi_repo_url
        }
        stage('notify success') {
            """
            TODO:
            slackSend(
                color: "good", channel: notify_channel,
                message: "Successfully uploaded airflow-provider-datarobot library to " + pypi_repo_url + ". \n" + build_info_msg
            )
            """
        }
    }
    catch(e) {
        stage('notify failure') {
            """
            TODO:
            String fail_msg = "Failed to upload the airflow-provider-datarobot library to " + pypi_repo_url + "."
            slackSend(
                color: "danger", channel: notify_channel,
                message: fail_msg + " \n " + build_info_msg + " \n " + e
            )
            """
            throw e
        }
    }
}