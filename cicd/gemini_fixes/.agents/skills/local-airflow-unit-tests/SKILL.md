---
name: local-airflow-unit-tests
description: Summarizes how to use docker to run a tagged Composer image for local testing.
---

# Local Airflow Unit Tests with Docker

This skill describes how to run Cloud Composer images locally using `docker` to execute unit tests in an environment that matches the production Composer environment.

## Prerequisites

-   `docker` installed and running.
-   Python 3 installed (to run the image tag helper script).

## Steps

### 1. Get the Composer Tagged Image

Use the provided script to get the fully qualified Docker image tag for the desired Composer version. This script reads from `cicd/composer_version.txt`.

```bash
# From the repository root
IMAGE_TAG=$(python3 cicd/get_composer_tagged_image.py)
echo $IMAGE_TAG
```

This will output something like:
`us-docker.pkg.dev/cloud-airflow-releaser/airflow-worker-scheduler-2-10-5/airflow-worker-scheduler-2-10-5:composer-2-airflow-2.10.5`

### 2. Run the Container with Docker

To run tests locally, you need to mount your workspace into the container so that it has access to your DAGs, requirements, and test files.

You can run the container interactively:

```bash
# Get the image tag
IMAGE_TAG=$(python3 cicd/get_composer_tagged_image.py)

# Run the container
docker run -it \
  -v $(pwd):/workspace \
  -w /workspace \
  --entrypoint /bin/bash \
  $IMAGE_TAG
```

### 3. Initialize and Run Tests (Inside the Container)

Once inside the container, you can set up the environment and run pytest by following the bash script below.
Make sure that all the test results are printed out to Cloud Build via standard output so that they're visible in the Cloud Build logs.

```bash
# Set up environment variables
export PYTHONUSERBASE=/home/airflow/.local
export PATH=$PYTHONUSERBASE/bin:$PATH
export AIRFLOW_HOME=/home/airflow/airflow
export AIRFLOW__CORE__DAGS_FOLDER=/workspace/cicd/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# (REQUIRED) Generate constraints file from pre-installed packages in the image
pip list --format=freeze > /tmp/constraints.txt

# Install workspace dependencies using constraints to avoid conflicts
pip install --user \
  -r /workspace/composer/requirements.txt \
  -c /tmp/constraints.txt

# In Airflow 3, some internal components (like the Simple Auth Manager) may attempt to initialize 
# or "build" UI assets on the fly if they aren't present. Since Airflow is installed in a system-level
# directory (/opt/python3.11/...), the airflow user doesn't have the rights to create new folders there.
sudo mkdir -p /opt/python3.11/lib/python3.11/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist
sudo chown -R airflow: /opt/python3.11/lib/python3.11/site-packages/airflow/api_fastapi/auth/managers/simple/ui/dist

# Set basic auth if testing Airflow 2 
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

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
```

### 4. Fix DAGs and Tests

Make any necessary Airflow DAG code corrections or refactors to get the tests passing.
Do not modify the semantic logic of any DAGs or tests.
Do not modify any thresholds or constants being checked in the tests.