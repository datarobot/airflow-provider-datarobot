import os
import datarobot as dr
from datarobot import Deployment


def main():
    print(f"Running python code: {__file__}")

    # Using this job runtime parameters
    print()
    print("Runtime parameters:")
    print("-------------------")
    string_param = os.environ.get("STRING_PARAMETER", None)
    print(f"string param: {string_param}")

    deployment_param = os.environ.get("DEPLOYMENT", None)
    print(f"deployment_param: {deployment_param}")

    model_package_param = os.environ.get("MODEL_PACKAGE", None)
    print(f"model_package_param: {model_package_param}")

    # An example of using the python client to list deployments
    deployments = Deployment.list()
    print()
    print("List of all deployments")
    print("-----------------------")
    for deployment in deployments:
        print(deployment)


if __name__ == "__main__":
    main()
