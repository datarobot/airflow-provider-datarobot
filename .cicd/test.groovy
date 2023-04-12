node('multi-executor && ubuntu:focal'){
  checkout scm
  sh 'bash run_bash_check.sh'
}