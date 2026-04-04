FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Layer 1: Airflow Amazon provider + boto3
# Must use constraints to avoid breaking pre-installed Airflow core packages.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt" \
    "apache-airflow-providers-amazon>=8.0.0,<9.0.0" \
    "boto3"

# Layer 2: dbt packages
# Separate layer — dbt deps don't touch Airflow internals so no constraints needed.
# dbt-core 1.8 requires Python >= 3.9; resolved by python3.11 base image above.
RUN pip install --no-cache-dir \
    "dbt-core>=1.8,<1.9" \
    "dbt-athena-community>=1.8,<1.9"

# Build from repo root so both paths are in context:
#   docker build -f short-video-lakehouse-airflow/Dockerfile -t short-video-lakehouse-airflow .
COPY dags/ /opt/airflow/dags/
COPY src/orchestration/ /opt/airflow/dags/orchestration/

# dbt project — used by run_dbt_quality_gates on AWS (--target aws hits Athena)
COPY dbt_project.yml /opt/airflow/dbt/dbt_project.yml
COPY profiles.yml    /opt/airflow/dbt/profiles.yml
COPY packages.yml    /opt/airflow/dbt/packages.yml
COPY models/         /opt/airflow/dbt/models/
COPY macros/         /opt/airflow/dbt/macros/
COPY tests_dbt/      /opt/airflow/dbt/tests_dbt/
RUN dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
