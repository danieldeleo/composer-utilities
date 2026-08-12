#!/bin/bash
set -e

export PYTHONUSERBASE=/home/airflow/.local
export PATH=$PYTHONUSERBASE/bin:$PATH
export AIRFLOW_HOME=/home/airflow/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/workspace/cicd/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

pip list --format=freeze > /tmp/constraints.txt

pip install --user \
  -r /workspace/cicd/dags/requirements.txt \
  -c /tmp/constraints.txt

sudo mkdir -p /opt/python3.11/lib/python3.11/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist
sudo chown -R airflow: /opt/python3.11/lib/python3.11/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist

export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

airflow standalone &

airflow db check

echo "Waiting for Airflow Webserver to be ready..."
until curl -sf http://localhost:8080 > /dev/null; do
    sleep 2
done
echo "Webserver is up!"

python3 -m pytest -vv -s /workspace/cicd/tests
