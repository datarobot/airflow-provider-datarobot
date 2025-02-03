# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
import logging

logger = logging.getLogger(__package__)

experimental_warning = (
    "You have imported from the _experimental directory.\n"
    "This directory is used for unreleased datarobot features.\n"
    "Unless you specifically know better,"
    " you don't have the access to use this functionality in the app, so this code will not work."
)

logger.warning(experimental_warning)
