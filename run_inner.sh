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
