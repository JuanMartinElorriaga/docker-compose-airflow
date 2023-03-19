FROM apache/airflow:2.3.0

# Install any linux packages here (Optional)

USER root

RUN apt-get update -yqq 
#&& apt-get install -y vim

# Add any python libraries  (Optional)
#https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html?highlight=snowflake

USER airflow

RUN pip install \
    'python-dotenv' \
    'neo4j' \
    'py2neo' \
    'plyvel' \
    'apache-airflow-providers-http' \
    'apache-airflow-providers-postgres' \
    'apache-airflow-providers-neo4j'

USER airflow