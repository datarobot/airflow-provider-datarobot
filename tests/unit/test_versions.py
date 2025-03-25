# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import os
import re

import pytest

from datarobot_provider import get_provider_info


@pytest.fixture
def version_file(root):
    return os.path.join(root, "datarobot_provider", "_version.py")


@pytest.fixture
def changes_file(root):
    return os.path.join(root, "CHANGES.md")


@pytest.fixture
def package_version(version_file):
    with open(version_file) as fd:
        version_search = re.search(
            r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE
        )
        version = version_search.group(1)
    return version


def test_primary_version_defined(package_version):
    assert package_version is not None


def test_airflow_entry_version_is_equivalent(package_version):
    provider_info = get_provider_info()
    assert [package_version] == provider_info["versions"]


def test_changes_file_is_formatted_correctly(changes_file):
    with open(changes_file) as fd:
        lines = fd.readlines()
        assert len(lines) > 0
        assert lines[0] == "# Changelog\n"
        assert lines[1] == "\n"
        assert lines[2] == "## Unreleased Changes\n"
        # Ensure there are not duplicate headings for unreleased changes
        for line in lines[3:]:
            assert line != "## Unreleased Changes\n"
