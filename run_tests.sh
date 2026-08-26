#!/bin/bash
IMAGE_TAG="us-docker.pkg.dev/cloud-airflow-releaser/airflow-worker-scheduler-2-11-1/airflow-worker-scheduler-2-11-1:composer-3-airflow-2.11.1-build.17"

cat << 'INNER_EOF' > run_inner.sh
# Set up environment variables
export PYTHONUSERBASE=/home/airflow/.local
export PATH=$PYTHONUSERBASE/bin:$PATH
export AIRFLOW_HOME=/home/airflow/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/workspace/cicd/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

# (REQUIRED) Generate constraints file from pre-installed packages in the image
pip list --format=freeze > /tmp/constraints.txt

# Install workspace dependencies using constraints to avoid conflicts
pip install --user pytest -r /workspace/cicd/dags/requirements.txt -c /tmp/constraints.txt

sudo mkdir -p /opt/python3.11/lib/python3.11/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist
sudo chown -R airflow: /opt/python3.11/lib/python3.11/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist

# Start Airflow standalone in the background
airflow standalone &

# Wait for the Airflow database to be fully initialized
airflow db check

# Wait for the Airflow Webserver to be ready to accept REST API connections
echo "Waiting for Airflow Webserver to be ready..."
until curl -sf http://localhost:8080 > /dev/null; do
    sleep 2
done
echo "Webserver is up!"

# Run pytest to execute the tests in the workspace
python3 -m pytest -vv -s /workspace/cicd/tests
INNER_EOF
chmod +x run_inner.sh

DOCKER_API_VERSION=1.41 docker run -v $(pwd):/workspace -w /workspace --entrypoint /bin/bash $IMAGE_TAG ./run_inner.sh
