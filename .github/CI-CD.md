
# CI — Code quality checks

 WHEN IT RUNS:
   Only on pull requests targeting the main branch.
   Reason: I don't want CI running on every feature branch push - that wastes
   minutes and creates noise. I only care that code is clean before it merges
   into main, which is the branch connected to the production CAPSTONE database.

 WHAT IT CHECKS:
   1. Ruff lint       - catches syntax errors, undefined names, unused imports
   2. Ruff format     - enforces consistent code style across the whole team
   3. DAG imports     - imports every DAG file to catch Airflow config errors
                        that would only surface at runtime otherwise
   4. dbt compile     - parses all models, macros, schema.yml files without
                        connecting to Snowflake or running any SQL
---

- name: Checkout code
# Get the code 
Reason: the runner starts as a blank Ubuntu machine — it knows nothing about the repo until this step pulls it down

- name: Set up Python 3.12
# Set up Python 
Reason: must match the Python version in your Dockerfile (3.12) so the import checks behave the same way locally and in CI

- name: Install Python dependencies
# Install dependencies
Reason: we only install what CI actually needs, not the full requirements.txt. Heavy packages like snowflake-connector require system libs that would slow CI to 10+ minutes. The DAG import check only needs Airflow + the specific providers the DAGs reference.

- name: Lint with Ruff
# Lint
Reason: catches bugs before they reach production;
E = code style errors
F = undefined names, unused imports (these cause runtime crashes)
W = warnings
dbt_project and terraform are excluded because they contain SQL and HCL files that Ruff doesn't understand

- name: Format check with Ruff
# Format check
Reason: --check mode tells you WHAT would change without changing it. Forces  developers to run `ruff format .` locally before a PR can merge. Consistent formatting means git diffs show only logic changes, not whitespace noise.

- name: DAG import check
# DAG import check
Reason: importing a DAG module runs Airflow's parser on it. This catches
syntax errors, missing providers, and bad operator config that would
only surface when the scheduler tries to load the DAG at runtime —
by which point it would silently disappear from the Airflow UI.

Dummy env vars are set so DAGs that call os.getenv() don't crash on import just because real credentials aren't present in CI.

- name: Install dbt
# nstall dbt
Reason: dbt is installed separately from the Airflow constraint step because dbt's dependency tree conflicts with Airflow constraints. Same reason it's in a separate RUN layer in the Dockerfile.

- name: Create CI profiles.yml
# Create a CI-only profiles.yml
Reason: dbt compile requires a profiles.yml to exist and be syntactically valid. It does NOT connect to Snowflake during compile, the credentials are dummy values. This means CI validates all your SQL and Jinja logic without touching your real CAPSTONE database.

# dbt deps
Reason: installs packages defined in packages.yml (e.g. dbt_utils). Must run before compile because compile resolves macros from packages.

# dbt compile
Reason: parses every model, macro, ref(), source(), and schema.yml without running any SQL against Snowflake. Catches:
   - Undefined ref() or source() - model references a table that doesn't exist
   - Undefined macros - you call a macro that isn't installed or defined
   - Bad Jinja syntax - {{ }} expressions that would crash at runtime
   - schema.yml referencing columns not in the model SELECT
This is the dbt equivalent of a compiler check — fast and safe.

---


# CD — Build and push Docker image to Docker Hub

 WHEN IT RUNS:
   Only when you publish a GitHub Release with a version tag.
   Format: v1.0.0, v1.2.3, v2.0.0

   Reason: we don't build and push on every merge to main because not every merge represents a deployable release. A release tag is a deliberate act which means "this version is ready to be deployed". This gives you control over what actually lands in production vs what is just merged and tested.

 HOW TO TRIGGER IT:
   Option A — GitHub UI:
     GitHub repo → Releases → Draft a new release → Tag: v1.0.0 → Publish

   Option B — Terminal:
     git tag v1.0.0
     git push origin v1.0.0

 IMAGE TAGS PRODUCED:
   adebola/dec-supplychain360-airflow:latest     ← always points to newest release
   adebola/dec-supplychain360-airflow:v1.0.0     ← pinned to this exact release
   adebola/dec-supplychain360-airflow:v1           ← major version pointer

  Reason for multiple tags: `latest` is convenient for pulling the newest
   version. The version tag (v1.0.0) allows you to roll back to any previous
   release with `docker pull adebola/dec-supplychain360-airflow:v1.0.0`.

 REQUIRED GITHUB SECRETS:
   Go to: GitHub repo → Settings → Secrets and variables → Actions

   DOCKERHUB_USERNAME   → adebola
   DOCKERHUB_TOKEN      → generate at hub.docker.com → Account Settings
                          → Security → New Access Token
                          


# Set up Docker Buildx
Reason: Buildx is an extended Docker build client that supports layer caching via a registry cache. Without Buildx, every CD run rebuilds every layer from scratch even if nothing changed that means re-downloading the 1GB Airflow base image every time. With registry cache, unchanged layers are reused and the build goes from ~8 minutes to ~2 minutes.

# Log in to Docker Hub
Reason: you must authenticate before you can push an image. Uses the DOCKERHUB_TOKEN secret, passowrd is not advisabble.

# Extract metadata and generate image tags
Reason: this action reads the git tag that triggered the workflow (e.g. v1.0.0) and automatically generates the correct Docker image tags from it — latest, v1.0.0, and v1. Doing this manually would mean hard-coding version strings and updating them every release — error-prone and easy to forget.

# Build and push
Reason: builds the image from your Dockerfile and pushes all three tags simultaneously in one operation.

cache-from / cache-to: uses Docker Hub itself as a layer cache store. On first run this has nothing to pull from. On subsequent runs it pulls cached layers for anything that hasn't changed (base image, pip installs) and only rebuilds the layers where your code actually changed (COPY dags/).

push: true means the image is pushed to Docker Hub immediately after build. In CI you always want push: true — building without pushing is pointless.

# Print summary
Reason: writes a summary to the GitHub Actions run page so you can see exactly what was pushed without digging through logs.

---