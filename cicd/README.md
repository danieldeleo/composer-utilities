# Cloud Composer CI/CD & Automation Suite

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2Fcomposer-utilities.git&cloudshell_workspace=control_panel%2F)

This directory contains the automation, testing, and continuous integration/continuous deployment (CI/CD) pipelines for managing Apache Airflow DAGs and dependencies in Google Cloud Composer environments.

---

## Directory Structure

```
cicd/
├── cloudbuild.yaml                  # Main Cloud Build CI/CD entrypoint pipeline
├── test_dags.yaml                   # Child Cloud Build configuration for containerized Airflow testing
├── composer_version.txt             # Target Cloud Composer & Airflow version definition
├── get_composer_tagged_image.py     # Python script to resolve the target environment base Docker image
├── dags/                            # Composer DAGs folder synchronized to environments
│   ├── requirements.txt             # Python dependencies installed in Cloud Composer
│   └── *.py                         # Airflow DAG python scripts
├── tests/                           # Testing suite for verifying DAGs
│   ├── unit/
│   │   └── test_unit.py             # Pytest suite validating DAG imports, start dates, and structures
│   └── integration/
│       └── test_integration.py      # Integration testing via local standalone Airflow REST API calls
└── gemini_fixes/                    # Antigravity CLI automated code analysis & optimization configuration
    ├── antigravitycli.yaml          # Cloud Build pipeline that uses Antigravity CLI to optimize code and create PRs
    └── .agents/
        └── skills/                  # Custom Antigravity CLI skills (Airflow best practices and unit testing)
```

---

## 1. Main CI/CD Pipeline (`cloudbuild.yaml`)

The main Cloud Build pipeline coordinates static checks, test execution inside matching Composer images, and deployment to the environments.

### Execution Steps
1. **Linting and Formatting Checks**: Runs `ruff check` and `ruff format --check` on Python files inside `cicd/` to enforce formatting and python style guides.
2. **Resolve Docker Image**: Executes `get_composer_tagged_image.py` to retrieve the Google Cloud-hosted Docker image corresponding to the target environment version configured in `composer_version.txt`.
3. **Trigger Testing Container**:
   - Replaces the placeholder `REPLACE_WITH_COMPOSER_TAGGED_IMAGE` in `test_dags.yaml` with the resolved image tag.
   - Spawns a nested/child Cloud Build job via `gcloud builds submit --config test_dags.yaml` to run unit and integration tests inside that container.
4. **Deploy and Synchronize**:
   - **Only runs if the nested testing steps pass.**
   - Performs a recursive synchronization using `gcloud storage rsync` to sync files from `cicd/dags/` to the `dags/` folder of all Composer environments in the target regions (defaulting to `us-east4` and `us-central1`).
   - Updates the PyPI packages on the environment using `gcloud composer environments update --update-pypi-packages-from-file cicd/dags/requirements.txt`, ignoring redundant calls if there are no package changes.

---

## 2. Test Execution Container (`test_dags.yaml`)

This configuration runs inside the resolved Cloud Composer Docker container to ensure exact runtime parity with production.

### Execution Steps
1. **Initialize Directory Permissions**: Pre-creates Airflow UI build asset directories with correct ownership (`airflow:`) to avoid write-permission failures under newer Airflow releases.
2. **Install Dependencies with Constraints**:
   - Freezes current container package versions to `/tmp/constraints.txt`.
   - Installs libraries specified in `cicd/dags/requirements.txt` using the frozen list as constraints (`--constraint /tmp/constraints.txt`) to prevent package conflict errors.
3. **Start Standalone Airflow**: Launches Airflow in standalone mode in the background and polls `airflow db check` and `http://localhost:8080` until the webserver is ready.
4. **Run Pytest**: Executes tests inside `/workspace/tests` (both unit and integration tests).

---

## 3. Testing Suite (`tests/`)

The test suite validates both static configurations and dynamic task executions.

### Unit Tests (`tests/unit/test_unit.py`)
- **DagBag Import Validation**: Checks that `DagBag` loads without syntax, import, or parsing errors.
- **DAG ID & File Alignment**: Verifies that each DAG's `dag_id` matches its Python file name.
- **Start Dates**: Checks that all DAGs define a static, fixed `start_date` rather than a dynamic one (e.g., `days_ago` or `datetime.now()`).
- **DAG Parsing SLA**: Checks the execution duration for the scheduler to parse each file, asserting that it completes within a threshold (10 seconds) to prevent CPU bottlenecks on the scheduler loop.
- **Mocks**: Out-of-the-box mocks are supplied for external GCP resources (BigQuery operators, GCS hooks, Environment snapshots, and Airflow variables) so execution checks don't require live GCP access.

### Integration Tests (`tests/integration/test_integration.py`)
- **API Token Retrieval**: Dynamically detects the Airflow major version. For Airflow 3, it fetches a JWT token via post requests; for Airflow 2, it falls back to basic auth using local configuration files.
- **DAG Unpausing**: Connects to the local Airflow REST API to programmatically unpause DAGs.
- **Trigger & Verify**: Triggers targeted DAG runs via the Airflow REST API and polls the state until it reaches either `success` or `failed`, validating operational flow.

---

## 4. Helper Scripts & Configuration

### `composer_version.txt`
Contains the target version string of the Cloud Composer environment (e.g. `composer-3-airflow-2.11.1-build.0`). **Keep this file updated to match your production environments.**

### `get_composer_tagged_image.py`
A Python script that reads `composer_version.txt`, parses the Composer/Airflow versions, and outputs the fully qualified Artifact Registry container image tag hosted in GCP (e.g. `us-docker.pkg.dev/cloud-airflow-releaser/...`).

---

## 5. Antigravity CLI Optimization Workflow (`gemini_fixes/`)

This submodule automates code analysis, applies Airflow best practices, and proposes changes via Pull Requests using the Antigravity CLI.

### Workflow Configuration (`antigravitycli.yaml`)
1. **Environment Setup**: Installs system dependencies and the GitHub CLI (`gh`).
2. **Install CLI**: Downloads and installs the Antigravity CLI via its installation script.
3. **AI Code Optimization**:
   - Mounts the local `.agents/skills/` directory.
   - Prompts Antigravity CLI to analyze files and optimize DAGs according to target guidelines.
4. **Automated PR**: Checks for changes. If modifications exist, it commits them to a new branch (`agy-fix-<build-id>`), pushes to GitHub, and uses the GitHub CLI (`gh`) to open a Pull Request against the base branch.

### Target Best Practices (`.agents/skills/airflow-best-practices/SKILL.md`)
The optimization tool validates and corrects DAG code against several Airflow best practices:
- **No Top-Level Database/API Calls**: Ensures database queries, environment variables fetches (`Variable.get()`), and network operations are confined within operator callables or templates to avoid overloading the scheduler during periodic DAG parsing.
- **Static Start Dates**: Replaces dynamic start dates.
- **Idempotent Tasks**: Configures write operations to be idempotent (UPSERTs, partition overwrites, or execution-date parameterized paths).
- **Lightweight XComs**: Flags heavy data transfers (like Pandas DataFrames) and replaces them with GCS/S3 storage paths.
- **Proper Retries**: Ensures standard retry count and retry delays are configured in `default_args`.

### Cleaning Up Stale Agent Branches

If you experiment with Antigravity CLI and branch tests, you might accumulate several `agy-fix-*` branches. You can run this bash script to batch delete them from the remote repository:

```bash
#!/usr/bin/env bash

# Fetch the latest remote branches and prune deleted ones
git fetch -p

# Find all remote branches starting with agy-fix-
# awk extracts just the branch name after 'origin/'
BRANCHES=$(git branch -r | awk -F'origin/' '/agy-fix-/{print $2}')

if [ -z "$BRANCHES" ]; then
  echo "No matching remote branches found. Nothing to clean up!"
  exit 0
fi

echo "Found the following remote branches to delete:"
echo "$BRANCHES"
echo

# Delete all of the branches at once
echo "$BRANCHES" | xargs git push origin --delete

echo "Cleanup complete!"
```
