FROM apache/airflow:2.9.3

# Install dbt in an isolated venv to avoid conflicts with Airflow's dependencies
USER root
RUN python -m venv /opt/dbt-env && \
    /opt/dbt-env/bin/pip install --no-cache-dir "dbt-redshift<2.0" && \
    chmod -R a+rx /opt/dbt-env

# Install Slack provider into Airflow's own environment using its constraint file
USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    apache-airflow-providers-slack