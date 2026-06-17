# Composer Utilities
This repo contains experimental utilities to help users of [Cloud Composer](https://cloud.google.com/composer).

For samples, please see [Python Docs Samples](https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/composer) which contains code samples found in [Composer documentation](cloud.google.com/composer)

These tools are NOT under any kind of SLO or SLA and have limited support. 

## [Environment Diff Tool](./environment_diff)
Takes in two Managed Airflow environments and compares their attributes

## [Managed Airflow (Gen1 to Gen2) Migration Tool](./migration/gen1_to_gen2)
Takes in a Managed Airflow environment and analyzes the DAGs for compatibility with Composer 2

## [Managed Airflow DAGs Parsing Profiler Tool](./dag_parsing_profiler)
Profiles DAG parsing for a Managed Airflow environment. This tool helps you optimize parsing latency, including top-level code detection

## [Cloud Composer CI/CD Pipeline](./cicd)
A template and sample configuration for establishing a CI/CD pipeline for Cloud Composer. It features:
* **Linting & Formatting**: Checks using Ruff.
* **Automated Testing**: Runs unit and integration tests against local Airflow instances inside a Composer-matching environment.
* **Deployment**: Automatically syncs DAGs, data files, and dependencies (`requirements.txt`) to Cloud Composer environments upon successful validation.
* **Agentic Remediation**: An optional setup using Gemini CLI to automatically analyze, fix, and propose PRs for DAG failures or optimizations.
