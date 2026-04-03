
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


# Get the code
- name: Checkout code

Reason: the runner starts as a blank Ubuntu machine It knows nothing about the repo until this step pulls it down


# Set up Python
- name: Set up Python 3.12

Reason:this must match the Python version in Dockerfile (3.12) so the import checks behave the same way locally and in CI


# Install dependencies
- name: Install Python dependencies

Reason: I only install what CI actually needs, not the full requirements.txt. Heavy packages like snowflake-connector require system libs that would slow CI to 10+ minutes. The DAG import check only needs Airflow and the specific providers the DAGs reference.


# Lint
- name: Lint with Ruff

Reason: catches bugs before they reach production;

E = code style errors

F = undefined names, unused imports that causes runtime crashes

# Format check
W = warnings
dbt_project and terraform are excluded because they contain SQL and HCL files that Ruff doesn't understand

- name: Format check with Ruff
Reason: check mode tells WHAT would change without changing it. Forces the script writer/me to run `ruff format . and ruff check . --fix` locally before a pull request can merge. Consistent formatting means git diffs show only logic changes, not whitespace noise.


# DAG import check
- name: DAG import check

Reason: importing a DAG module runs Airflow's parser on it. It catches syntax errors, missing providers, and bad operator config that would only surface when the scheduler tries to load the DAG at runtime by which point it would silently disappear from the Airflow UI.

I added dummy env vars to DAGs that call os.getenv() so it doesn't crash on import just because real credentials aren't present in CI.


# Install dbt
- name: Install dbt

Reason: dbt is installed separately from the Airflow constraint step because dbt's dependency tree conflicts with Airflow constraints. Same reason I made it a separate RUN layer in the Dockerfile.


# Create a CI-only profiles.yml
- name: Create CI profiles.yml

Reason: dbt compile requires a profiles.yml to exist and be syntactically valid. It does NOT connect to Snowflake during compile, the credentials are dummy values. This means CI validates all my SQL and Jinja logic without touching my actual CAPSTONE database.

# dbt deps
Reason: installs packages defined in packages.yml (e.g. dbt_utils). It must run before compile because compile resolves macros from packages.

# dbt compile
Reason: parses every model, macro, ref(), source(), and schema.yml without running any SQL against Snowflake. It will catch:
   - Undefined ref() or source() - model referencing a table that doesn't exist.
   - Undefined macros - Any macro that isn't installed or defined in my macros.
   - Bad Jinja syntax - {{ }} expressions that would crash at runtime.
   - schema.yml referencing columns not in the model.

---


# CD — Build and push Docker image to Docker Hub

 WHEN IT RUNS:
   Only when I publish a GitHub Release with a version tag.
   e.g: v1.0.0, v1.2.3, v2.0.0

   Reason: I don't want to build and push on every merge to main because not every merge represents a deployable release. A release tag is a deliberate act which means "this version is ready to be deployed". This gives me control over what actually lands in production vs what is just merged and tested.

 HOW TO TRIGGER IT:
    GitHub UI:
     GitHub repo → Releases → Draft a new release → Tag: v1.0.0 → Publish


# Print summary
Reason: writes a summary to the GitHub Actions run page so I can see exactly what was pushed.

---