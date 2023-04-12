#!/usr/bin/env bash
set -xe

export REPOSITORY_URL=$1
echo "REPOSITORY_URL: $REPOSITORY_URL"

virtualenv .venv -p python3.8
source .venv/bin/activate

PYTHON_VERSION="$(python3 --version)"
echo "Python Version: $PYTHON_VERSION"

pip install -r requirements.txt

echo "Installing dependencies needed to build/push python packages..."
pip install -U pip
pip install --upgrade build
pip install -U twine

echo "Building wheel..."
#make clean
python -m build

echo "Built artifacts..."
# TODO

echo "Building docs..."
# TODO

echo "Upload python packages to $REPOSITORY_URL..."
# TODO

echo "Upload release docs"
# TODO

echo "Finished successfully!"