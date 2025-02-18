# Changelog

## Unreleased Changes
- Add `hospital_readmissions_xgboost_example.py` DAG.

## 0.1.0

### New features
- Introduce `CreateRegisteredModelVersionOperator <datarobot_provider.operators.CreateRegisteredModelVersionOperator>`
to create registered models that are generic containers that group multiple versions of models which can be deployed
- Introduce `CreateWranglingRecipeOperator <datarobot_provider.operators.ai_catalog.CreateWranglingRecipeOperator>`
and `CreateDatasetFromRecipeOperator <datarobot_provider.operators.ai_catalog.CreateDatasetFromRecipeOperator>`
to create a wrangling recipe and publish it as a dataset into an existing use case.
- Introduce `CreateDatasetFromProjectOperator <datarobot_provider.operators.ai_catalog.CreateDatasetFromProjectOperator>`
to create datasets from project data.
- Add `GetDataStoreOperator <datarobot_provider.operators.connections.GetDataStoreOperator>` to work directly with existing DataRobot data connections.
- Make `CreateOrUpdateDataSourceOperator <datarobot_provider.operators.ai_catalog.CreateOrUpdateDataSourceOperator>` `dataset_name` parameter optional.
- Make `CreateOrUpdateDataSourceOperator <datarobot_provider.operators.ai_catalog.CreateOrUpdateDataSourceOperator>` parameters use templates.
- Add `hospital_readmissions_example.py` DAG.
- Add `feature_discovery_example.py` DAG.
- Add an optional *use_case_id* parameter into `CreateProjectOperator <datarobot_provider.operators.ai_catalog.datarobot.CreateProjectOperator>`
- Add `GetDataStoreOperator <datarobot_provider.operators.datarobot.GetProjectBlueprintsOperator>` to get blueprint ids for a project.

### Bugfixes

### API changes

### Deprecation summary

### Documentation changes

### Experimental changes

- Introduce `NotebookRunOperator <datarobot_provider._experimental.operators.notebook.NotebookRunOperator>`
and `NotebookRunCompleteSensor <datarobot_provider._experimental.sensors.notebook.NotebookRunCompleteSensor>`

## 0.0.12

### New features

Specified supported Python versions to >=3.9

## 0.0.11

### Bugfixes

Fixes typo which lead to a bug in UpdateBiasAndFairnessSettingsOperator.

### API changes

UpdateBiasAndFairnessSettingsOperator now accepts param fairness_metric_set instead of fairness_metrics_set.

### Deprecation summary

Param UpdateBiasAndFairnessSettingsOperator.fairness_metrics_set is deprecated, and going to be removed in the next minor release.
