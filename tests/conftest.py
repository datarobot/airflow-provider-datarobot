# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def root():
    return Path(__file__).parent.parent


@pytest.fixture(scope='session')
def tests_dir(root):
    return root / 'tests'


@pytest.fixture(scope='session')
def provider_dir(root):
    return root / 'datarobot_provider'
