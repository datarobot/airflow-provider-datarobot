# Copyright 2022 DataRobot, Inc. and its affiliates.
#
# All rights reserved.
#
# This is proprietary source code of DataRobot, Inc. and its affiliates.
#
# Released under the terms of DataRobot Tool and Utility Agreement.
from collections.abc import Sequence
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot import Model
from datarobot.insights import ShapImpact
from datarobot.insights import ShapPreview
from datarobot.models import StatusCheckJob

from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator


class GetFeaturesUsedOperator(BaseDatarobotOperator):
    """
    Get the features used for training the model.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        List[str]: The names of the features used in the model.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required.")

        if not self.model_id:
            raise ValueError("model_id is required.")

    def execute(self, context: Context) -> List[str]:
        model = dr.models.Model.get(self.project_id, self.model_id)
        features = model.get_features_used()

        return features


class ComputeFeatureImpactOperator(BaseDatarobotOperator):
    """
    Creates Feature Impact job in DataRobot.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Feature Impact job ID.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required to compute Feature Impact.")

        if not self.model_id:
            raise ValueError("model_id is required to compute Feature Impact.")

    def execute(self, context: Context) -> str:
        model = dr.models.Model.get(self.project_id, self.model_id)

        job = model.request_feature_impact()

        self.log.info(f"Feature Impact Job submitted, job_id={job.id}")

        return job.id


class ComputeFeatureEffectsOperator(BaseDatarobotOperator):
    """
    Submit request to compute Feature Effects for the model.

    Args:
        project_id (str): DataRobot project ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        str: Feature Effects job ID.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.model_id = model_id

    def validate(self) -> None:
        if not self.project_id:
            raise ValueError("project_id is required to compute Feature Effects.")

        if not self.model_id:
            raise ValueError("model_id is required to compute Feature Effects.")

    def execute(self, context: Context) -> str:
        model = dr.models.Model.get(self.project_id, self.model_id)

        job = model.request_feature_effect()

        self.log.info(f"Feature Effects Job submitted, job_id={job.id}")

        return job.id


class ComputeShapPreviewOperator(BaseDatarobotOperator):
    """
    Creates SHAP preview job in DataRobot.

    Parameters
    ----------
    model_id : str
        DataRobot model ID
    datarobot_conn_id : str, optional
        Connection ID, defaults to `datarobot_default`

    Returns
    -------
    str
        SHAP preview result id
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "model_id",
    ]

    def __init__(
        self,
        *,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = str(model_id)

    def validate(self) -> None:
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def execute(self, context: Context) -> str:
        job: StatusCheckJob = ShapPreview.compute(entity_id=self.model_id)
        return job.job_id


class ComputeShapImpactOperator(BaseDatarobotOperator):
    """
    Creates SHAP impact job in DataRobot.

    Parameters
    ----------
    model_id : str
        DataRobot model ID
    datarobot_conn_id : str, optional
        Connection ID, defaults to `datarobot_default`

    Returns
    -------
    str
        SHAP impact job id
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "model_id",
    ]

    def __init__(
        self,
        *,
        model_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.model_id = str(model_id)

    def validate(self) -> None:
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def execute(self, context: Context) -> str:
        job: StatusCheckJob = ShapImpact.compute(entity_id=self.model_id)
        return job.job_id


class GetRocCurveInsightOperator(BaseDatarobotOperator):
    """
    Creates ROC curve insight data.

    Args:
        project_id (str): DataRobot model ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        Dict[str, Any]: Roc curve points, positive class predictions, negative class predictions.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        source: Optional[str] = "validation",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = str(project_id)
        self.model_id = str(model_id)
        self.source = str(source)

    def validate(self) -> None:
        if not self.project_id:
            raise AirflowFailException("The `project_id` parameter is required.")
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def get_model(self) -> Model:
        model: Model = Model.get(self.project_id, self.model_id)
        if not model:
            raise AirflowFailException(
                f"Model with id {self.model_id} not found in project {self.project_id}."
            )
        return model

    def execute(self, context: Context) -> Dict[str, Any]:
        model = self.get_model()
        roc_data = model.get_roc_curve(source=self.source)
        return {
            "roc_points": roc_data.roc_points,
            "positive_class_predictions": roc_data.positive_class_predictions,
            "negative_class_predictions": roc_data.negative_class_predictions,
        }


class GetLiftChartInsightOperator(BaseDatarobotOperator):
    """
    Creates Lift Chart insight data.

    Args:
        project_id (str): DataRobot model ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        List[Dict[str, Any]]: List of the lift chart bins containing the actuals and predicted values.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        source: Optional[str] = "validation",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = str(project_id)
        self.model_id = str(model_id)
        self.source = str(source)

    def validate(self) -> None:
        if not self.project_id:
            raise AirflowFailException("The `project_id` parameter is required.")
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def get_model(self) -> Model:
        model: Model = Model.get(self.project_id, self.model_id)
        if not model:
            raise AirflowFailException(
                f"Model with id {self.model_id} not found in project {self.project_id}."
            )
        return model

    def execute(self, context: Context) -> List[Dict[str, Any]]:
        model = self.get_model()
        lift_chart = model.get_lift_chart(source=self.source)
        return lift_chart.bins


class GetResidualsChartInsightOperator(BaseDatarobotOperator):
    """
    Creates Residuals Chart insight data.

    Args:
        project_id (str): DataRobot model ID.
        model_id (str): DataRobot model ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        Dict[str, float]: The residual chart values for the model.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "project_id",
        "model_id",
    ]

    def __init__(
        self,
        *,
        project_id: str,
        model_id: str,
        source: Optional[str] = "validation",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = str(project_id)
        self.model_id = str(model_id)
        self.source = str(source)

    def validate(self) -> None:
        if not self.project_id:
            raise AirflowFailException("The `project_id` parameter is required.")
        if not self.model_id:
            raise AirflowFailException("The `model_id` parameter is required.")

    def get_model(self) -> Model:
        model: Model = Model.get(self.project_id, self.model_id)
        if not model:
            raise AirflowFailException(
                f"Model with id {self.model_id} not found in project {self.project_id}."
            )
        return model

    def execute(self, context: Context) -> Dict[str, float]:
        model = self.get_model()
        residuals = model.get_residuals_chart(source=self.source)
        return {
            "residual_mean": residuals.residual_mean,
            "coefficient_of_determination": residuals.coefficient_of_determination,
            "standard_deviation": residuals.standard_deviation,
        }
