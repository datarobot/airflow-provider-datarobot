# Copyright 2023 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import List
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot import DatetimePartitioningSpecification
from datarobot import Project

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator

DATAROBOT_MAX_WAIT = 600


class AutopilotBaseOperator(BaseDatarobotOperator):
    @staticmethod
    def start_autopilot(
        project_id,
        autopilot_settings,
        partitioning_settings=None,
        datetime_partitioning_settings=None,
        advanced_options=None,
        featurelist_id=None,
        relationships_configuration_id=None,
        segmentation_task_id=None,
        max_wait_sec=DATAROBOT_MAX_WAIT,
    ):
        """
        Triggers DataRobot Autopilot to train set of models.

        Args:
            project_id (str): DataRobot project ID.
            autopilot_settings (dict): Autopilot settings.
            partitioning_settings (dict, optional): Partitioning settings.
            datetime_partitioning_settings (dict, optional): Datetime partitioning settings.
            advanced_options (dict, optional): Advanced options.
            featurelist_id (str, optional): Specifies which feature list to use.
            relationships_configuration_id (str, optional): ID of the relationships configuration to use.
            segmentation_task_id (str, optional): The segmentation task that should be used to split the project for
                segmented modeling.
            max_wait_sec (int, optional): For some settings, an asynchronous task must be run to analyze the dataset.
                max_wait governs the maximum time (in seconds) to wait before giving up.
            datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
        """

        project = dr.Project.get(project_id)
        if project.target:
            raise AirflowFailException(f"Autopilot has already started for project_id={project.id}")

        if featurelist_id:
            autopilot_settings["featurelist_id"] = featurelist_id

        if relationships_configuration_id:
            autopilot_settings["relationships_configuration_id"] = relationships_configuration_id

        if segmentation_task_id:
            autopilot_settings["segmentation_task_id"] = segmentation_task_id

        autopilot_settings["max_wait"] = max_wait_sec

        if datetime_partitioning_settings and partitioning_settings:
            raise AirflowFailException(
                "parameters: datetime_partitioning_settings and partitioning_settings are mutually exclusive"
            )

        if datetime_partitioning_settings:
            if isinstance(datetime_partitioning_settings, dict):
                project.set_datetime_partitioning(**datetime_partitioning_settings)
            else:
                project.set_datetime_partitioning(
                    datetime_partition_spec=datetime_partitioning_settings
                )
        elif partitioning_settings:
            project.set_partitioning_method(**partitioning_settings)

        if advanced_options:
            project.set_options(**advanced_options)

        # finalize the project and start the autopilot
        project.analyze_and_model(**autopilot_settings)


class StartAutopilotOperator(AutopilotBaseOperator):
    """
    Triggers DataRobot Autopilot to train set of models.

    Args:
        project_id (str): DataRobot project ID.
        featurelist_id (str, optional): Specifies which feature list to use.
        relationships_configuration_id (str, optional): ID of the relationships configuration to use.
        segmentation_task_id (str, optional): The segmentation task that should be used to split the project for
            segmented modeling.
        max_wait_sec (int, optional): For some settings, an asynchronous task must be run to analyze the dataset.
            max_wait governs the maximum time (in seconds) to wait before giving up.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "featurelist_id",
        "relationships_configuration_id",
        "segmentation_task_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        featurelist_id: Optional[str] = None,
        relationships_configuration_id: Optional[str] = None,
        segmentation_task_id: Optional[str] = None,
        max_wait_sec: int = DATAROBOT_MAX_WAIT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.featurelist_id = featurelist_id
        self.relationships_configuration_id = relationships_configuration_id
        self.segmentation_task_id = segmentation_task_id
        self.max_wait_sec = max_wait_sec

    def execute(self, context: Context) -> None:
        self.log.info(f"Starting DataRobot Autopilot for project_id={self.project_id}")
        self.start_autopilot(
            project_id=self.project_id,
            autopilot_settings=context["params"]["autopilot_settings"],
            partitioning_settings=context["params"].get("partitioning_settings"),
            datetime_partitioning_settings=context["params"].get("datetime_partitioning_settings"),
            advanced_options=context["params"].get("advanced_options"),
            featurelist_id=self.featurelist_id,
            relationships_configuration_id=self.relationships_configuration_id,
            segmentation_task_id=self.segmentation_task_id,
            max_wait_sec=DATAROBOT_MAX_WAIT,
        )


class StartDatetimeAutopilotOperator(AutopilotBaseOperator):
    """
    Configure the project as a datetime partitioned project. This operator should be run after
    the CreateProjectOperator and before training models. It will return a

    Args:
        project_id (str): DataRobot project ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
        datetime_partition_column (str): The name of the column whose values as dates are used to assign a
            row to a particular partition.
        featurelist_id (str, optional): Specifies which feature list to use.
        segmentation_task_id (str, optional): The segmentation task that should be used to split the project for
            segmented modeling.
        autopilot_data_selection_method (str): One of `datarobot.enums.DATETIME_AUTOPILOT_DATA_SELECTION_METHOD`.
            Whether models created by the autopilot should use "rowCount" or "duration" as their data_selection_method.
        validation_duration (str or None): The default validation_duration for the backtests.
        holdout_start_date (datetime.datetime or None): The start date of holdout scoring data. If `holdout_start_date`
            is specified, either `holdout_duration` or `holdout_end_date` must also be specified. If `disable_holdout`
            is set to `True`, `holdout_start_date`, `holdout_duration`, and `holdout_end_date` may not be specified.
        holdout_duration (str or None): The duration of the holdout scoring data. If `holdout_duration` is specified,
            `holdout_start_date` must also be specified. If `disable_holdout` is set to `True`, `holdout_duration`,
            `holdout_start_date`, and `holdout_end_date` may not be specified.
        holdout_end_date (datetime.datetime or None): The end date of holdout scoring data. If `holdout_end_date` is
            specified, `holdout_start_date` must also be specified. If `disable_holdout` is set to `True`,
            `holdout_end_date`, `holdout_start_date`, and `holdout_duration` may not be specified.
        disable_holdout (bool or None): Whether to suppress allocating a holdout fold. If set to `True`,
            `holdout_start_date`, `holdout_duration`, and `holdout_end_date` may not be specified.
        gap_duration (str or None): The duration of the gap between training and holdout scoring data.
        number_of_backtests (int or None): The number of backtests to use.
        backtests (list of datarobot.BacktestSpecification): The exact specification of backtests to use. The indices
            of the specified backtests should range from 0 to number_of_backtests - 1. If any backtest is left
            unspecified, a default configuration will be chosen.
        use_time_series (bool): Whether to create a time series project (if `True`) or an OTV project which uses
            datetime partitioning (if `False`). The default behavior is to create an OTV project.
        default_to_known_in_advance (bool): Optional, default `False`. Used for time series projects only. Sets
            whether all features default to being treated as known in advance. Known in advance features are expected
            to be known for dates in the future when making predictions, e.g., "is this a holiday?". Individual
            features can be set to a value different than the default using the `feature_settings` parameter.
        default_to_do_not_derive (bool): Optional, default `False`. Used for time series projects only. Sets whether
            all features default to being treated as do-not-derive features, excluding them from feature derivation.
            Individual features can be set to a value different than the default by using the `feature_settings`
            parameter.
        feature_derivation_window_start (int or None): Only used for time series projects. Offset into the past to
            define how far back relative to the forecast point the feature derivation window should start. Expressed
            in terms of the `windows_basis_unit` and should be negative value or zero.
        feature_derivation_window_end (int or None): Only used for time series projects. Offset into the past to define
            how far back relative to the forecast point the feature derivation window should end. Expressed in terms of
            the `windows_basis_unit` and should be a negative value or zero.
        feature_settings (list of datarobot.FeatureSettings): Optional, a list specifying per feature settings, can be
            left unspecified.
        forecast_window_start (int or None): Only used for time series projects. Offset into the future to define how
            far forward relative to the forecast point the forecast window should start. Expressed in terms of the
            `windows_basis_unit`.
        forecast_window_end (int or None): Only used for time series projects. Offset into the future to define how far
            forward relative to the forecast point the forecast window should end. Expressed in terms of the
            `windows_basis_unit`.
        windows_basis_unit (str, optional): Only used for time series projects. Indicates which unit is a basis for
        feature derivation window and forecast window. Valid options are detected time unit (one of the
            `datarobot.enums.TIME_UNITS`) or "ROW". If omitted, the default value is the detected time unit.
        treat_as_exponential (str, optional): Defaults to "auto". Used to specify whether to treat data as exponential
            trend and apply transformations like log-transform. Use values from the
            `datarobot.enums.TREAT_AS_EXPONENTIAL` enum.
        differencing_method (str, optional): Defaults to "auto". Used to specify which differencing method to apply of
            case if data is stationary. Use values from `datarobot.enums.DIFFERENCING_METHOD` enum.
        periodicities (list of datarobot.Periodicity, optional): A list of `datarobot.Periodicity`. Periodicities units
            should be "ROW", if the `windows_basis_unit` is "ROW".
        multiseries_id_columns (List[str] or None): A list of the names of multiseries id columns to define series
            within the training data. Currently only one multiseries id column is supported.
        use_cross_series_features (bool): Whether to use cross series features.
        aggregation_type (Optional[str]): The aggregation type to apply when creating cross series features. Optional,
            must be one of "total" or "average".
        cross_series_group_by_columns (list of Optional[str]): List of columns (currently of length 1). Optional setting
            that indicates how to further split series into related groups. For example, if every series is sales of an
            individual product, the series group-by could be the product category with values like "men's clothing",
            "sports equipment", etc. Can only be used in a multiseries project with `use_cross_series_features` set to
            `True`.
        calendar_id (Optional[str]): The id of the `datarobot.CalendarFile` to use with this project.
        unsupervised_mode (Optional[bool]): Defaults to False, indicates whether partitioning should be constructed for
            the unsupervised project.
        model_splits (Optional[int]): Sets the cap on the number of jobs per model used when building models to control
            number of jobs in the queue. Higher number of model splits will allow for less downsampling leading to the
            use of more post-processed data.
        allow_partial_history_time_series_predictions (Optional[bool]): Whether to allow time series models to make
            predictions using partial historical data.
        unsupervised_type (Optional[str]): The unsupervised project type, only valid if `unsupervised_mode` is True. Use
            values from `datarobot.enums.UnsupervisedTypeEnum` enum. If not specified then the project defaults to
            'anomaly' when `unsupervised_mode` is True.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id", "datetime_partition_column", "use_time_series"]

    def __init__(
        self,
        *,
        project_id: str,
        datetime_partition_column: str,
        use_time_series: bool,
        featurelist_id: Optional[str] = None,
        segmentation_task_id: Optional[str] = None,
        autopilot_data_selection_method: Optional[str] = None,
        validation_duration: Optional[str] = None,
        holdout_start_date: Optional[str] = None,
        holdout_duration: Optional[str] = None,
        disable_holdout: Optional[bool] = None,
        gap_duration: Optional[str] = None,
        number_of_backtests: Optional[int] = None,
        backtests: Any = None,
        default_to_known_in_advance: bool = False,
        default_to_do_not_derive: bool = False,
        feature_derivation_window_start: Optional[int] = None,
        feature_derivation_window_end: Optional[int] = None,
        feature_settings: Any = None,
        forecast_window_start: Optional[int] = None,
        forecast_window_end: Optional[int] = None,
        windows_basis_unit: Optional[str] = None,
        treat_as_exponential: Optional[str] = None,
        differencing_method: Optional[str] = None,
        periodicities: Any = None,
        multiseries_id_columns: Optional[List[str]] = None,
        use_cross_series_features: Optional[bool] = None,
        aggregation_type: Optional[str] = None,
        cross_series_group_by_columns: Optional[List[str]] = None,
        calendar_id: Optional[str] = None,
        holdout_end_date: Optional[str] = None,
        unsupervised_mode: bool = False,
        model_splits: Optional[int] = None,
        allow_partial_history_time_series_predictions: bool = False,
        unsupervised_type: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.datetime_partition_column = datetime_partition_column
        self.use_time_series = use_time_series
        self.featurelist_id = featurelist_id
        self.segmentation_task_id = segmentation_task_id
        self.autopilot_data_selection_method = autopilot_data_selection_method
        self.validation_duration = validation_duration
        self.holdout_start_date = holdout_start_date
        self.holdout_duration = holdout_duration
        self.disable_holdout = disable_holdout
        self.gap_duration = gap_duration
        self.number_of_backtests = number_of_backtests
        self.backtests = backtests
        self.default_to_known_in_advance = default_to_known_in_advance
        self.default_to_do_not_derive = default_to_do_not_derive
        self.feature_derivation_window_start = feature_derivation_window_start
        self.feature_derivation_window_end = feature_derivation_window_end
        self.feature_settings = feature_settings
        self.forecast_window_start = forecast_window_start
        self.forecast_window_end = forecast_window_end
        self.windows_basis_unit = windows_basis_unit
        self.treat_as_exponential = treat_as_exponential
        self.differencing_method = differencing_method
        self.periodicities = periodicities
        self.multiseries_id_columns = multiseries_id_columns
        self.use_cross_series_features = use_cross_series_features
        self.aggregation_type = aggregation_type
        self.cross_series_group_by_columns = cross_series_group_by_columns
        self.calendar_id = calendar_id
        self.holdout_end_date = holdout_end_date
        self.unsupervised_mode = unsupervised_mode
        self.model_splits = model_splits
        self.allow_partial_history_time_series_predictions = (
            allow_partial_history_time_series_predictions
        )
        self.unsupervised_type = unsupervised_type

    def execute(self, context: Context) -> None:
        project = Project.get(self.project_id)
        if project.target:
            self.log.info(f"Project has already been started for project_id={project.id}")
        else:
            datetime_partition_spec = self.get_datetime_partition_spec()

            self.start_autopilot(
                project_id=self.project_id,
                autopilot_settings=context["params"]["autopilot_settings"],
                partitioning_settings=None,
                datetime_partitioning_settings=datetime_partition_spec,
                advanced_options=context["params"].get("advanced_options"),
                featurelist_id=self.featurelist_id,
                relationships_configuration_id=None,
                segmentation_task_id=self.segmentation_task_id,
                max_wait_sec=DATAROBOT_MAX_WAIT,
            )

    def get_datetime_partition_spec(self):
        return DatetimePartitioningSpecification(
            use_time_series=self.use_time_series,
            datetime_partition_column=self.datetime_partition_column,
            autopilot_data_selection_method=self.autopilot_data_selection_method,
            validation_duration=self.validation_duration,
            holdout_start_date=self.holdout_start_date,
            holdout_duration=self.holdout_duration,
            disable_holdout=self.disable_holdout,
            gap_duration=self.gap_duration,
            number_of_backtests=self.number_of_backtests,
            backtests=self.backtests,
            default_to_known_in_advance=self.default_to_known_in_advance,
            default_to_do_not_derive=self.default_to_do_not_derive,
            feature_derivation_window_start=self.feature_derivation_window_start,
            feature_derivation_window_end=self.feature_derivation_window_end,
            feature_settings=self.feature_settings,
            forecast_window_start=self.forecast_window_start,
            forecast_window_end=self.forecast_window_end,
            windows_basis_unit=self.windows_basis_unit,
            treat_as_exponential=self.treat_as_exponential,
            differencing_method=self.differencing_method,
            periodicities=self.periodicities,
            multiseries_id_columns=self.multiseries_id_columns,
            use_cross_series_features=self.use_cross_series_features,
            aggregation_type=self.aggregation_type,
            cross_series_group_by_columns=self.cross_series_group_by_columns,
            calendar_id=self.calendar_id,
            holdout_end_date=self.holdout_end_date,
            unsupervised_mode=self.unsupervised_mode,
            model_splits=self.model_splits,
            allow_partial_history_time_series_predictions=self.allow_partial_history_time_series_predictions,
            unsupervised_type=self.unsupervised_type,
        )
