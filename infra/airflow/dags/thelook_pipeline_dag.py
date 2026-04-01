"""
TheLook Lakehouse — Daily Transformation Pipeline

Astronomer Cosmos renders each dbt model as an individual Airflow task.
Execution: LOCAL mode — dbt runs directly inside the Airflow container.

DAG graph: start >> dbt_deps >> staging >> intermediate >> mart >> end
"""

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="thelook_lakehouse",
    install_dbt_deps=False,
    manifest_path=DBT_PROJECT_PATH / "target" / "manifest.json",
)

profile_config = ProfileConfig(
    profile_name="thelook_trino",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/opt/dbt_venv/bin/dbt",
)

with DAG(
    dag_id="thelook_dbt_pipeline",
    description="Daily dbt transformations — Bronze → Silver → Gold",
    schedule="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["thelook", "dbt", "lakehouse"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Install dbt packages (dbt_utils) before running models
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && /opt/dbt_venv/bin/dbt deps",
    )

    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/staging"],
        ),
    )

    intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/intermediate"],
        ),
    )

    mart = DbtTaskGroup(
        group_id="mart",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["path:models/mart"],
        ),
    )

    start >> dbt_deps >> staging >> intermediate >> mart >> end
