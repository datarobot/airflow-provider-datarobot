# Changelog

## Unreleased Changes

## v0.5.0
- Fix support for datarobot 3.7.0 sdk.
- Implement operators to retrieve target drift and feature drift
- Update readme to reflect changes to installation environment
- Add documentation for examples
- Corrections throughout documentation.

## v0.4.1
- Update all release pipelines to improve releasing to pypi.
- Add safety checks to release pipelines.

## v0.4.0
- Add documentation for `post_predictions_formatting.py`.
- Add documentation for `deployment_prediction_generation.py` example DAG.
- Update the `model_training_xgboost.py` documentation.
- Add the `CrossValidationMetricsOperator` to compute scoring for all partitions for a model.
- Add the `ScoreBacktestsModelOperator` to score all backtests for a datetime partitioned model.
- Add the `GetFeaturesUsedOperator` to get the features used to train the model.

## 0.3.0
- Rename example DAGs to product/GTM desired names
- Add `CancelJobOperator`
- Add segmentation task create operator, tests and docs
- Add get model parameters operator and tests
- Added Operator and DAG to run custom functions
- Add example creation feature discovery retraining and scoring
- Operator to return a list of ids and type for all trained models in project
- Add operator to change advanced tuning parameters on models
- Convert final sensor docstrings to correct format
- Add residuals operator, tests, and docs
- Update docstrings to Google Docstring format for datarobot operators
- Remove outdated Feature Discovery operators.
- Add DB example for data prep
- Add get roc curve operator
- Add lift chart, tests and docs
- Add replace the model operator
- Add OTV and Time Series start autopilot operator
- Adding predictions generation operators and sensor

## 0.2.0
- Add `hospital_readmissions_xgboost_example.py` example DAG.
- Add `hospital_readmissions_deployment_prediction_generation.py` example DAG.
- Update `TrainModelsOperator` to latest sdk.
- Add updated `TrainModelOperator` for training single models.
- Add `GetProjectBlueprintsOperator` for getting blueprint ids for a project.
- Add `DeployRegisteredModelOperator` for deploying registered models.
- Add `UpdateDriftTrackingOperator` for updating drift tracking settings on deployments.
- Update `ComputeFeatureImpactOperator` to latest sdk compatibility.
- Updated `ComputeFeatureEffectsOperator` to latest sdk compatibility.
- Add `ComputeShapPreviewOperator` to compute SHAP previews for a model.
- Add `ComputeShapImpactOperator` to compute SHAP impact for a model.

## 0.1.0

### New features
- Introduce `CreateRegisteredModelVersionOperator <datarobot_provider.operators.CreateRegisteredModelVersionOperator>` to create registered models that are generic containers that group multiple versions of models which can be deployed
- Introduce `CreateWranglingRecipeOperator <datarobot_provider.operators.data_prep.CreateWranglingRecipeOperator>` and `CreateDatasetFromRecipeOperator <datarobot_provider.operators.data_prep.CreateDatasetFromRecipeOperator>` to create a wrangling recipe and publish it as a dataset into an existing use case.
- Introduce `CreateDatasetFromProjectOperator <datarobot_provider.operators.data_registry.CreateDatasetFromProjectOperator>` to create datasets from project data.
- Add `GetDataStoreOperator <datarobot_provider.operators.connections.GetDataStoreOperator>` to work directly with existing DataRobot data connections.
- Make `CreateOrUpdateDataSourceOperator <datarobot_provider.operators.data_registry.CreateOrUpdateDataSourceOperator>` `dataset_name` parameter optional.
- Make `CreateOrUpdateDataSourceOperator <datarobot_provider.operators.data_registry.CreateOrUpdateDataSourceOperator>` parameters use templates.
- Add `hospital_readmissions_example.py` DAG.
- Add `feature_discovery_example.py` DAG.
- Add an optional *use_case_id* parameter into `CreateProjectOperator <datarobot_provider.operators.datarobot.CreateProjectOperator>`
- Add `GetDataStoreOperator <datarobot_provider.operators.datarobot.GetProjectBlueprintsOperator>` to get blueprint ids for a project.

### Experimental changes

- Introduce `NotebookRunOperator <datarobot_provider._experimental.operators.notebook.NotebookRunOperator>` and `NotebookRunCompleteSensor <datarobot_provider._experimental.sensors.notebook.NotebookRunCompleteSensor>`

## 0.0.12

### New features

- Specified supported Python versions to >=3.9

## 0.0.11

### Bugfixes

- Fixes typo which lead to a bug in UpdateBiasAndFairnessSettingsOperator.

### API changes

- UpdateBiasAndFairnessSettingsOperator now accepts param fairness_metric_set instead of fairness_metrics_set.

### Deprecation summary

- Param UpdateBiasAndFairnessSettingsOperator.fairness_metrics_set is deprecated, and going to be removed in the next minor release.
