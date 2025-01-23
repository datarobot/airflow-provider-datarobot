# Changelog

## Unreleased

### New features
- Introduce `CreateWranglingRecipeOperator <datarobot_provider.operators.ai_catalog.CreateWranglingRecipeOperator>` 
and `CreateDatasetFromRecipeOperator <datarobot_provider.operators.ai_catalog.CreateDatasetFromRecipeOperator>` 
to create a wranglig recipe and publish it as a dataset into an existing experiment container. 

### Bugfixes

### API changes

### Deprecation summary

### Documentation changes

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
