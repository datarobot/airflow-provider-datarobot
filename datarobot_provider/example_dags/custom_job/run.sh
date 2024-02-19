#!/bin/bash

echo "Job Starting: ($0)"

echo "===== Runtime Parameters ======"
echo "Model Package:     $MODEL_PACKAGE"
echo "Deployment:        $DEPLOYMENT"
echo "STRING_PARAMETER:  $STRING_PARAMETER"
echo
echo
echo "===== Generic Variables ==========================="
echo "CURRENT_CUSTOM_JOB_RUN_ID: $CURRENT_CUSTOM_JOB_RUN_ID"
echo "CURRENT_CUSTOM_JOB_ID:     $CURRENT_CUSTOM_JOB_ID"
echo "DATAROBOT_ENDPOINT:        $DATAROBOT_ENDPOINT"
echo "DATAROBOT_API_TOKEN:       Use the environment variable $DATAROBOT_API_TOKEN"
echo "==================================================="

echo
echo "How to check how much memory your job has"
  memory_limit_bytes=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
  memory_limit_megabytes=$((memory_limit_bytes / 1024 / 1024))
echo "Memory Limit (in Megabytes): $memory_limit_megabytes"
echo

# Uncomment the following if you want to check if the job has network access
## Define the IP address of an external server to ping (e.g., Google's DNS)
#external_server="8.8.8.8"
#echo "Checking internet connection"
## Try to ping the external server
#ping -c 1 $external_server > /dev/null 2>&1
#
## Check the exit status of the ping command
#if [ $? -eq 0 ]; then
#    echo "Internet connection is available."
#else
#    echo "No internet connection."
#fi
#echo
#echo

# Run the code in job.py
dir_path=$(dirname $0)
echo "Entrypoint is at $dir_path - cd into it"
cd $dir_path

if command -v python3 &>/dev/null; then
    echo "python3 is installed and available."
else
    echo "Error: python3 is not installed or not available."
    exit 1
fi

python_file="job.py"
if [ -f "$python_file" ]; then
    echo "Found $python_file .. running it"
    python3 ./job.py
else
    echo "File $python_file does not exist"
    exit 1
fi
