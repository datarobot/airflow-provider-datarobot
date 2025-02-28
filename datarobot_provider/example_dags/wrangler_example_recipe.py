# Copyright 2025 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.

from typing import Any
from typing import Dict
from typing import List

WRANGLER_EXAMPLE_RECIPE: List[Dict[str, Any]] = [
    {
        "directive": "rename-columns",
        "arguments": {
            "columnMappings": [
                {"originalName": "admission_type_id", "newName": "AdmissionType"},
                {
                    "originalName": "discharge_disposition_id",
                    "newName": "DischargeDisposition",
                },
                {"originalName": "admission_source_id", "newName": "AdmissionSource"},
            ]
        },
    },
    {
        "directive": "replace",
        "arguments": {
            "origin": "AdmissionType",
            "searchFor": "",
            "replacement": "Not Available",
            "matchMode": "exact",
        },
    },
    {
        "directive": "replace",
        "arguments": {
            "searchFor": r"\\[(\\d+).*",
            "replacement": r"$1",
            "origin": "age",
            "matchMode": "regex",
        },
    },
    {
        "directive": "compute-new",
        "arguments": {
            "expression": "CAST(`age` AS Integer)",
            "newFeatureName": "int-age",
        },
    },
    {
        "directive": "drop-columns",
        "arguments": {
            "columns": [
                "citoglipton",
                "acetohexamide",
                "miglitol",
                "troglitazone",
                "examide",
                "age",
            ]
        },
    },
]
