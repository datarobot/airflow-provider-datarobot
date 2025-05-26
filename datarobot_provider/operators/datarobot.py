# Copyright 2022 DataRobot, Inc. and its affiliates.
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
from typing import Tuple
from typing import Union

import datarobot as dr
from airflow.exceptions import AirflowFailException
from airflow.utils.context import Context
from datarobot import Blueprint
from strenum import StrEnum

from datarobot_provider.operators.base_datarobot_operator import XCOM_DEFAULT_USE_CASE_ID
from datarobot_provider.operators.base_datarobot_operator import BaseDatarobotOperator
from datarobot_provider.operators.base_datarobot_operator import BaseUseCaseEntityOperator

DATAROBOT_MAX_WAIT = 3600
DATAROBOT_AUTOPILOT_TIMEOUT = 86400
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%s"


class GetOrCreateUseCaseOperator(BaseDatarobotOperator):
    """
    Creates a DataRobot Use Case.

    Args:
        datarobot_conn_id (str): Connection ID, defaults to `datarobot_default`.
        name (str): Use Case name.
        description (Optional[str]): Use Case description.
        reuse_policy (CreateUseCaseOperator.ReusePolicy): Should the operator reuse an existing Use Case with the same *name*?

            EXACT: Reuse the Use Case if it has exactly the same *name* and *description*.
            SEARCH_BY_NAME_UPDATE_DESCRIPTION: Reuse the Use Case if it has exactly the same *name*. Update *description* if it's different.
            SEARCH_BY_NAME_PRESERVE_DESCRIPTION: Reuse the Use Case if it has exactly the same *name*. Don't modify *description*.
            NO_REUSE: Always create a new Use Case.

            default: EXACT.
        set_default (bool): Set this Use Case as a default one for all subsequent tasks in the DAG.

    Returns:
        str: DataRobot UseCase ID.
    """

    class ReusePolicy(StrEnum):
        EXACT = "EXACT"
        SEARCH_BY_NAME_UPDATE_DESCRIPTION = "SEARCH_BY_NAME_UPDATE_DESCRIPTION"
        SEARCH_BY_NAME_PRESERVE_DESCRIPTION = "SEARCH_BY_NAME_PRESERVE_DESCRIPTION"
        NO_REUSE = "NO_REUSE"

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["name", "description"]

    def __init__(
        self,
        *,
        name: str = "{{ params.use_case_name | default('Airflow') }}",
        description: Optional[str] = "{{ params.use_case_description | default('') }}",
        reuse_policy: ReusePolicy = ReusePolicy.EXACT,
        set_default: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.reuse_policy = reuse_policy
        self.set_default = set_default

    def execute(self, context: Context) -> Optional[str]:
        use_case = None

        if self.reuse_policy != self.ReusePolicy.NO_REUSE:
            use_case = self._search_for_existing_use_case()

            if (
                use_case is not None
                and self.reuse_policy == self.ReusePolicy.SEARCH_BY_NAME_UPDATE_DESCRIPTION
                and use_case.description != self.description
            ):
                self.log.info(
                    'Update Use Case description from "%s" to "%s"',
                    use_case.description,
                    self.description,
                )
                use_case.update(self.name, self.description)

        if use_case is None:
            self.log.info("Creating DataRobot Use Case")
            use_case = dr.UseCase.create(name=self.name, description=self.description)
            self.log.info(f"Use case created: use_case_id={use_case.id}")

        if self.set_default:
            self.log.info(
                'Set "%(name)s" (id=%(use_case_id)s) as a default Use Case.',
                {"name": use_case.name, "use_case_id": use_case.id},
            )
            self.xcom_push(context, XCOM_DEFAULT_USE_CASE_ID, use_case.id)

        return use_case.id

    def _search_for_existing_use_case(self) -> Optional[dr.UseCase]:
        candidates = []

        for use_case in dr.UseCase.list(search_params={"search": self.name}):
            if use_case.name == self.name:
                if (
                    self.reuse_policy == self.ReusePolicy.EXACT
                    and self.description
                    and use_case.description != self.description
                ):
                    continue

                self.log.info("Use an existing Use Case id=%s", use_case.id)

                candidates.append(use_case)

        if len(candidates) == 0:
            return None

        if (
            len(candidates) > 1
            and self.description
            and any(x.description == self.description for x in candidates)
        ):
            candidates = [x for x in candidates if x.description == self.description]

        return max(candidates, key=lambda x: x.created_at)


class CreateProjectOperator(BaseUseCaseEntityOperator):
    """
    Creates DataRobot project.

    Args:
        dataset_id (str, optional): DataRobot AI Catalog dataset ID.
        dataset_version_id (str, optional): DataRobot AI Catalog dataset version ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
        recipe_id (str, optional): DataRobot Recipe ID.

    Returns:
        str: DataRobot project ID.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = [
        "dataset_id",
        "dataset_version_id",
        "credential_id",
        "recipe_id",
        "use_case_id",
    ]

    def __init__(
        self,
        *,
        dataset_id: Optional[str] = None,
        dataset_version_id: Optional[str] = None,
        credential_id: Optional[str] = None,
        recipe_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.dataset_version_id = dataset_version_id
        self.credential_id = credential_id
        self.recipe_id = recipe_id

    def execute(self, context: Context) -> Optional[str]:
        use_case = self.get_use_case(context)

        # Create DataRobot project
        self.log.info("Creating DataRobot project")

        if self.dataset_id is None and "training_data" in context["params"]:
            # training_data may be a pre-signed URL to a file on S3 or a path to a local file
            project: dr.Project = dr.Project.create(
                context["params"]["training_data"],
                context["params"]["project_name"],
                use_case=use_case,
            )
            self.log.info(f"Project created: project_id={project.id} from local file")
            project.unsupervised_mode = context["params"].get("unsupervised_mode")
            project.unlock_holdout()
            return project.id
        elif self.dataset_id is not None or "training_dataset_id" in context["params"]:
            # training_dataset_id may be provided via params
            # or dataset_id should be returned from previous operator
            training_dataset_id = (
                self.dataset_id
                if self.dataset_id is not None
                else context["params"]["training_dataset_id"]
            )

            project = dr.Project.create_from_dataset(
                dataset_id=training_dataset_id,
                dataset_version_id=self.dataset_version_id,
                credential_id=self.credential_id,
                project_name=context["params"]["project_name"],
                use_case=use_case,
            )
            # Some weird problem with mypy: it passes here locally, but fails in CI
            self.log.info(
                f"Project created: project_id={project.id} from dataset: dataset_id={training_dataset_id}"  # type: ignore[attr-defined, unused-ignore]
            )
            return project.id  # type: ignore[attr-defined, unused-ignore]

        elif self.recipe_id is not None:
            project = dr.Project.create_from_recipe(
                recipe_id=self.recipe_id,
                use_case=use_case,
            )
            self.log.info(
                f"Project created: project_id={project.id} from recipe: recipe_id={self.recipe_id}"  # type: ignore[attr-defined, unused-ignore]
            )
            return project.id  # type: ignore[attr-defined, unused-ignore]

        else:
            raise AirflowFailException(
                "For Project creation one of training_data, training_dataset_id or "
                "recipe_id must be provided"
            )


class TrainModelsOperator(BaseDatarobotOperator):
    """
    Triggers DataRobot Autopilot to train models.

    Args:
        project_id (str): DataRobot project ID.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id"]

    def __init__(
        self,
        *,
        project_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id

    def execute(self, context: Context) -> None:
        # Train models
        project = dr.Project.get(self.project_id)
        if project.target:
            self.log.info(f"Models are already trained for project_id={project.id}")
        else:
            self.log.info(
                f"Starting DataRobot Autopilot for project_id={project.id} "
                f"with settings={context['params']['autopilot_settings']}"
            )
            project.analyze_and_model(**context["params"]["autopilot_settings"])


class SelectBestModelOperator(BaseDatarobotOperator):
    """
    Selects the best model from a DataRobot project based on a specified evaluation metric.

    If the evaluation metric is not provided, the operator uses the project's primary metric.

    Args:
        project_id (str): DataRobot project ID.
        metric (str, optional): The evaluation metric used to rank models.

    Returns:
        str: The best model's ID as a string.
    """

    template_fields = ["project_id"]

    def __init__(self, *, project_id: str, metric: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.metric = metric

    def validate(self) -> None:
        if not self.project_id:
            raise AirflowFailException("The `project_id` parameter is required.")

    def execute(self, context: Context) -> str:
        self.log.info(f"Selecting top model for project_id: {self.project_id}")
        project = dr.Project.get(self.project_id)

        if not self.metric:
            self.metric = project.metric
        best_model = project.get_top_model(metric=self.metric)
        return str(best_model.id)


class GetProjectModelsOperator(BaseDatarobotOperator):
    """
    Returns a list of all trained models and their ids.

    Args:
        project_id (str): DataRobot project ID.

    Returns:
        List[Tuple[str, str]]: The best model's ID as a string.
    """

    template_fields = ["project_id"]

    def __init__(self, *, project_id: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id

    def validate(self) -> None:
        if not self.project_id:
            raise AirflowFailException("The `project_id` parameter is required.")

    def execute(self, context: Context) -> List[Tuple[str, str]]:
        self.log.info(f"Selecting top model for project_id: {self.project_id}")
        project = dr.Project.get(self.project_id)
        models = project.get_models()

        return [(str(model.id), str(model.model_type)) for model in models]


class GetProjectBlueprintsOperator(BaseDatarobotOperator):
    """
    Get DataRobot project blueprints. Blueprint ids can optionally
    be filtered by a specific model type.

    Args:
        project_id (str): DataRobot project ID.
        return_all (bool, optional): Return all blueprint ids, default is False.
        filter_model_type (str, optional): Blueprint filter type, default returns all ids.
        datarobot_conn_id (str, optional): Connection ID, defaults to `datarobot_default`.

    Returns:
        list of str: List of DataRobot blueprint IDs.
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields: Sequence[str] = ["project_id"]

    def __init__(
        self,
        *,
        project_id: str,
        return_all: Optional[bool] = False,
        filter_model_type: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.filter_model_type = filter_model_type
        self.return_all = return_all

    def execute(self, context: Context) -> Union[List[Union[str, None]], str]:
        # Get DataRobot project blueprints
        project = dr.Project.get(self.project_id)
        blueprints: List[Blueprint] = project.get_blueprints()
        if self.filter_model_type:
            # Filter blueprints by type
            blueprint_ids = [
                blueprint.id
                for blueprint in blueprints
                if self.filter_model_type in str(blueprint.model_type).lower()
            ]
        else:
            blueprint_ids = [blueprint.id for blueprint in blueprints]

        if blueprint_ids is None:
            raise AirflowFailException(f"No blueprints found for project_id={self.project_id}")

        if self.return_all:
            return blueprint_ids
        else:
            return str(blueprint_ids[0])
