---
name: databricks-job-yaml-gen
description: >
  Generates a complete, production-grade Databricks Workflow YAML (DAB fragment) from a folder
  of sequenced notebooks. Use this skill whenever a user wants to: create a Databricks job from
  notebooks, generate a Databricks Asset Bundle (DAB) YAML, automate an ADF-triggered pipeline,
  wire up job-level parameters for a multi-step notebook workflow, set up conditional step
  logic (STEPID), or chain parameters between pipeline steps. Also triggers when the user
  mentions phrases like "create job YAML", "notebook pipeline config", "Databricks workflow
  from folder", or "ADF-triggered Databricks job". Always use this skill when a Databricks
  workspace folder scan + YAML output is involved — even if the user only says "make a job
  for this folder".
---

# Databricks Job YAML Generator (Production Grade)

Generates a complete, valid Databricks Workflow YAML for a sequenced notebook workflow
triggered from Azure Data Factory (ADF). Includes:

- Hardcoded cluster ID (simple, no DAB variable resolution required)
- Job-level parameters with consistent `s_` / `t_` naming
- Per-task `base_parameters` mapping to exact notebook widget names
- Conditional STEPID logic using the TRUE/FALSE branch pattern with skip tasks
- Parameter chaining between dependent steps
- Inline comments for long-term maintainability
- Pre-save validation checklist
- Optional one-shot job creation via Jobs API

---

## Invocation

```
@databricks-job-yaml-gen /Workspace/Users/<your-path>/<folder-name>
```

The path after the skill name is the **root folder** to scan. Claude will look for `fixed/`
inside it automatically, falling back to the folder itself if notebooks live there directly.

---

## Overview of the 10-step process

1. Get the current user's email
2. Scan the folder and list notebooks
3. Read each notebook and extract parameters + data flow
4. Define job-level parameters with suffix-on-multiplicity naming
5. Map job parameters → notebook parameters per task
6. Chain parameters between dependent steps
7. Build conditional STEPID logic using TRUE/FALSE branch pattern
8. Assemble job configuration with hardcoded cluster ID
9. Generate commented YAML, validate against checklist, and save
10. Optionally create the job in the workspace

---

## STEP 1 — Get user email

```python
current_user = spark.sql("SELECT current_user()").first()[0]
```

Use this email for `email_notifications.on_success` and `on_failure`.

---

## STEP 2 — Scan the folder

Resolution order:

1. If `<folder_path>/fixed/` exists → use it.
2. Else if `<folder_path>` contains notebooks directly → use that folder.
3. Else → ask the user:
   *"I couldn't find a `fixed/` subfolder or notebooks in `<folder_path>`. Please provide the path to the folder containing your notebooks."*
   Wait for their response.

Then:

- List all notebooks in the resolved folder
- Extract `step_id` from each name: `Step01_xxx → 1`, `Step07_xxx → 7`
- Sort ascending by `step_id`
- Print: `found N notebooks with step_ids [x, y, z]`

---

## STEP 3 — Read each notebook and extract

For every notebook, extract:

### a) Parameters

| Pattern | Extract |
|---|---|
| `dbutils.widgets.text("name", "default")` | param name + default |
| `dbutils.widgets.get("name")` | param name, default = `""` |
| `dbutils.widgets.dropdown("name", "default", [...])` | param name + default |
| `dbutils.widgets.combobox("name", "default", [...])` | param name + default |
| `dbutils.widgets.multiselect("name", "default", [...])` | param name + default |
| `getArgument("name", "default")` | param name + default |

Use the **exact** param name as written in code.

### b) Source tables

Look for:
- `spark.table(f"{catalog}.{schema}.{table}")`
- `spark.read.table(...)`
- `spark.read.format("delta").table(...)`
- `spark.read.load(...)`
- `spark.sql("SELECT * FROM ...")` (including f-strings referencing widgets)

Note which parameters feed into the source table reference.

### c) Target tables

Look for:
- `df.write.saveAsTable(...)`
- `df.write.format("delta").save(...)`
- `spark.sql("INSERT INTO ...")`
- `spark.sql("MERGE INTO ...")`

Note which parameters feed into the target table reference.

### d) Data flow

For each step, record:
- Which params are **source** (read)?
- Which params are **target** (write)?
- Does this step's source come from the previous step's target? (parameter chaining)

---

## STEP 4 — Create job-level parameters

### Core rule: suffix-on-multiplicity

- If the **entire job** has exactly 1 source table → use `s_source` (no suffix).
  If the **entire job** has 2+ source tables → use `s_source1`, `s_source2`, … (all suffixed).
- Same rule for targets: 1 → `t_target`; 2+ → `t_target1`, `t_target2`.
- Same for companions: 1 → `s_catalog`, `s_schema`; 2+ → `s_catalog1`, `s_schema1`, etc.

### Numbering is **global across the whole job**, not per-step

If Step01 reads table A and Step02 reads a different independent table B, they become
`s_source1` and `s_source2` at the job level (2 total sources in the job).

### Environment params collapse to one

All env-like params (`env`, `environment`, `ENV`) map to a single job param `environment`.

### Naming reference

| Purpose | 1 instance in job | 2+ instances in job |
|---|---|---|
| Step ID | `STEPID` | `STEPID` |
| Environment | `environment` | `environment` |
| Source catalog | `s_catalog` | `s_catalog1`, `s_catalog2`, … |
| Source schema | `s_schema` | `s_schema1`, `s_schema2`, … |
| Source table | `s_source` | `s_source1`, `s_source2`, … |
| Target catalog | `t_catalog` | `t_catalog1`, `t_catalog2`, … |
| Target schema | `t_schema` | `t_schema1`, `t_schema2`, … |
| Target table | `t_target` | `t_target1`, `t_target2`, … |
| Any other param | **Exact name** from notebook code | **Exact name** from notebook code |

### Defaults

- Use actual default values from code; use `""` if no default found
- `STEPID` default = `"1"`
- `environment` default = `"dev"`

---

## STEP 5 — Map job parameters → notebook parameters

In each task's `base_parameters`:

```
<exact_notebook_param_name>: "{{job.parameters.<job_param_name>}}"
```

### Example: standard names, single source/target

```python
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_catalog", "dev_edf_bronze")
dbutils.widgets.text("source_table", "MY_TABLE")
dbutils.widgets.text("target_catalog", "dev_edf_silver")
dbutils.widgets.text("target_table", "MY_TABLE_CURRENT")
```

→
```yaml
base_parameters:
  env: "{{job.parameters.environment}}"
  source_catalog: "{{job.parameters.s_catalog}}"
  source_table: "{{job.parameters.s_source}}"
  target_catalog: "{{job.parameters.t_catalog}}"
  target_table: "{{job.parameters.t_target}}"
```

### Example: custom notebook param names

```python
dbutils.widgets.text("src_cat", "dev_edf_bronze")
dbutils.widgets.text("src_tbl", "MY_TABLE")
dbutils.widgets.text("tgt_tbl", "MY_TABLE_CURRENT")
```

→
```yaml
base_parameters:
  src_cat: "{{job.parameters.s_catalog}}"
  src_tbl: "{{job.parameters.s_source}}"
  tgt_tbl: "{{job.parameters.t_target}}"
```

**KEY RULE: Left side = exact notebook param name. Never rename it.**

---

## STEP 6 — Parameter chaining between steps

If StepN's source is the **same table** Step(N-1) wrote to → map StepN's source params to
Step(N-1)'s **target** job params, and add an inline comment to make the chain explicit:

```yaml
base_parameters:
  # Chained from Step01 output:
  source_catalog: "{{job.parameters.t_catalog}}"
  source_schema:  "{{job.parameters.t_schema}}"
  source_table:   "{{job.parameters.t_target}}"
  target_table:   "{{job.parameters.t_target2}}"
```

If Step01 wrote multiple targets and Step02 reads them all:

```yaml
base_parameters:
  # Chained from Step01 outputs:
  source_table:  "{{job.parameters.t_target1}}"
  source_table2: "{{job.parameters.t_target2}}"
```

Only chain when the code **actually** shows the dependency. Independent sources get their own
`s_sourceN` slots.

---

## STEP 7 — Conditional STEPID logic (TRUE/FALSE Branch Pattern)

### Semantic meaning of STEPID (document in YAML header)

`STEPID` is the **starting step number**. The job runs every step whose
`step_id >= STEPID`. The condition `STEPID <= current_step_id` answers:
*"Has the user asked to start at or before this step?"*

- `STEPID=1` → start from step 1 → runs everything
- `STEPID=3` → start from step 3 → skips earlier steps
- `STEPID=7` → start from step 7 → only step 7 onward runs

---

### 🚨 CRITICAL — The TRUE/FALSE Branch Pattern 🚨

Databricks propagates "Excluded" status through dependency chains. When a condition
evaluates to FALSE, the TRUE-branch task is "Excluded", and that exclusion cascades
to ALL downstream tasks — even those with `run_if: ALL_DONE` or
`run_if: AT_LEAST_ONE_SUCCESS` when they depend on the excluded task.

**The ONLY reliable pattern** is:

1. Every condition gate has BOTH a TRUE branch (actual notebook) AND a FALSE branch
   (no-op "skip" task that always evaluates to true).
2. The NEXT gate depends on both the notebook task AND the skip task's `outcome: "true"`,
   with `run_if: AT_LEAST_ONE_SUCCESS`.
3. This guarantees exactly ONE path always completes and feeds forward.

**Why other patterns fail:**
- `depends_on` with both `outcome: "true"` AND `outcome: "false"` of same condition →
  non-taken branch marks downstream as "Excluded" even with `AT_LEAST_ONE_SUCCESS`
- `run_if: ALL_DONE` on downstream of excluded task → still gets excluded (never scheduled)
- Bridge tasks depending on excluded tasks → also get excluded

---

### Skip task definition

A skip task is a trivial `condition_task` that always evaluates to TRUE:

```yaml
- task_key: skip_step_N
  depends_on:
    - task_key: check_step_N
      outcome: "false"
  condition_task:
    op: LESS_THAN_OR_EQUAL
    left: "1"
    right: "1"
```

This task:
- Only runs when the gate evaluates to FALSE (step should be skipped)
- Always produces `outcome: "true"` (since 1 <= 1)
- Feeds into the next gate as a successful dependency

---

### Single step → no condition tasks

One `notebook_task`, no `depends_on`, no `run_if`, no skip tasks.

---

### Two-step job

```yaml
tasks:
  # === STEP 1 BLOCK ===
  - task_key: check_step_1
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "1"

  - task_key: Step01_Task
    depends_on:
      - task_key: check_step_1
        outcome: "true"
    existing_cluster_id: "1015-164218-68x6l8o"
    notebook_task: { ... }

  - task_key: skip_step_1
    depends_on:
      - task_key: check_step_1
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "1"
      right: "1"

  # === STEP 2 BLOCK (last step) ===
  - task_key: check_step_2
    depends_on:
      - task_key: Step01_Task
      - task_key: skip_step_1
        outcome: "true"
    run_if: AT_LEAST_ONE_SUCCESS
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "2"

  - task_key: Step02_Task
    depends_on:
      - task_key: check_step_2
        outcome: "true"
    existing_cluster_id: "1015-164218-68x6l8o"
    max_retries: 3
    min_retry_interval_millis: 2000
    notebook_task: { ... }

  - task_key: skip_step_2
    depends_on:
      - task_key: check_step_2
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "1"
      right: "1"
```

---

### Multiple steps (3+ steps)

For steps with step_ids [S1, S2, S3, ..., SN]:

```yaml
tasks:
  # === STEP S1 BLOCK (first — no depends_on on gate) ===
  - task_key: check_step_S1
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "S1"

  - task_key: StepS1_Task
    depends_on:
      - task_key: check_step_S1
        outcome: "true"
    existing_cluster_id: "1015-164218-68x6l8o"
    notebook_task: { ... }

  - task_key: skip_step_S1
    depends_on:
      - task_key: check_step_S1
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "1"
      right: "1"

  # === STEP S2 BLOCK ===
  - task_key: check_step_S2
    depends_on:
      - task_key: StepS1_Task
      - task_key: skip_step_S1
        outcome: "true"
    run_if: AT_LEAST_ONE_SUCCESS
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "S2"

  - task_key: StepS2_Task
    depends_on:
      - task_key: check_step_S2
        outcome: "true"
    existing_cluster_id: "1015-164218-68x6l8o"
    notebook_task: { ... }

  - task_key: skip_step_S2
    depends_on:
      - task_key: check_step_S2
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "1"
      right: "1"

  # === STEP S3 BLOCK ===
  - task_key: check_step_S3
    depends_on:
      - task_key: StepS2_Task
      - task_key: skip_step_S2
        outcome: "true"
    run_if: AT_LEAST_ONE_SUCCESS
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "S3"

  - task_key: StepS3_Task
    depends_on:
      - task_key: check_step_S3
        outcome: "true"
    existing_cluster_id: "1015-164218-68x6l8o"
    notebook_task: { ... }

  - task_key: skip_step_S3
    depends_on:
      - task_key: check_step_S3
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "1"
      right: "1"

  # ... repeat for each intermediate step ...

  # === LAST STEP SN BLOCK ===
  - task_key: check_step_SN
    depends_on:
      - task_key: Step<SN-1>_Task
      - task_key: skip_step_<SN-1>
        outcome: "true"
    run_if: AT_LEAST_ONE_SUCCESS
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "SN"

  - task_key: StepSN_Task
    depends_on:
      - task_key: check_step_SN
        outcome: "true"
    existing_cluster_id: "1015-164218-68x6l8o"
    max_retries: 3
    min_retry_interval_millis: 2000
    notebook_task: { ... }

  - task_key: skip_step_SN
    depends_on:
      - task_key: check_step_SN
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "1"
      right: "1"
```

---

### Dependency wiring summary table

| Task | depends_on | run_if |
|---|---|---|
| `check_step_<first>` (first gate) | nothing | (default) |
| `Step<first>_Task` | `check_step_<first>` outcome `"true"` | (default) |
| `skip_step_<first>` | `check_step_<first>` outcome `"false"` | (default) |
| `check_step_N` (N > first) | `Step<prev>_Task` (no outcome) + `skip_step_<prev>` outcome `"true"` | **`AT_LEAST_ONE_SUCCESS`** |
| `StepN_Task` (any) | `check_step_N` outcome `"true"` | (default) |
| `skip_step_N` (any) | `check_step_N` outcome `"false"` | (default) |

### Last step behavior

The last step's gate uses `right: "<highest_step_id>"`. Since STEPID will always be
<= highest_step_id for valid inputs, the last step always runs. Include `skip_step_N`
for the last step as a safety net (in case someone passes an invalid STEPID > max).

### Flow visualization

```
STEPID=1 (all steps run):
  check_step_1 (TRUE) → Step01 (runs) ──────────────────┐
  skip_step_1 (excluded)                                 │ AT_LEAST_ONE_SUCCESS
                                                         ▼
  check_step_2 (TRUE) → Step02 (runs) ──────────────────┐
  skip_step_2 (excluded)                                 │ AT_LEAST_ONE_SUCCESS
                                                         ▼
  check_step_3 (TRUE) → Step03 (runs) → ... → StepN ✅

STEPID=2 (skip step 1):
  check_step_1 (FALSE) → skip_step_1 (TRUE) ───────────┐
  Step01 (excluded)                                      │ AT_LEAST_ONE_SUCCESS
                                                         ▼
  check_step_2 (TRUE) → Step02 (runs) ──────────────────┐
  skip_step_2 (excluded)                                 │ AT_LEAST_ONE_SUCCESS
                                                         ▼
  check_step_3 (TRUE) → Step03 (runs) → ... → StepN ✅

STEPID=3 (skip steps 1 and 2):
  check_step_1 (FALSE) → skip_step_1 (TRUE) ───────────┐
  Step01 (excluded)                                      │ AT_LEAST_ONE_SUCCESS
                                                         ▼
  check_step_2 (FALSE) → skip_step_2 (TRUE) ───────────┐
  Step02 (excluded)                                      │ AT_LEAST_ONE_SUCCESS
                                                         ▼
  check_step_3 (TRUE) → Step03 (runs) → ... → StepN ✅
```

---

## STEP 8 — Job configuration

### Cluster ID — hardcoded default

Use the hardcoded default cluster ID `1015-164218-68x6l8o` on every notebook task.

If the user explicitly provides a different cluster ID during the conversation, use that
instead. Otherwise do **not** prompt — silently use the default.

Do **not** use DAB variables (`${var.cluster_id}`) or a `variables:` block. The cluster ID
is written as a plain string literal so the YAML works when pasted directly into the
Databricks UI or posted to the Jobs API without requiring `databricks bundle deploy`.

### YAML structure

```yaml
resources:
  jobs:
    Sqn_<TABLE_NAME>_<folder_name>:
      name: Sqn_<TABLE_NAME>_<folder_name>
      email_notifications:
        on_success: [<user_email>]
        on_failure: [<user_email>]
      timeout_seconds: 86400
      queue:
        enabled: true
      parameters: [...]
      tasks:
        - task_key: ...
          existing_cluster_id: "1015-164218-68x6l8o"
```

Derive `TABLE_NAME` from the main table name found in the notebook names/content.

---

## STEP 9 — Generate, validate, and save YAML

### Commenting strategy (5 layers)

**Layer 1 — File header** (purpose, trigger source, STEPID semantics, regeneration hint):

```yaml
# ============================================================================
# Sqn_<TABLE_NAME>_<folder_name>
# ----------------------------------------------------------------------------
# Sequenced notebook pipeline for <TABLE_NAME>, triggered by Azure Data Factory.
#
# ADF passes all parameters at job level. STEPID controls the starting step:
#   STEPID=N runs every step whose step_id >= N.
#
# STEPID Pattern: TRUE/FALSE Branch with Skip Tasks
#   Each condition gate has both a TRUE branch (notebook runs) and a FALSE
#   branch (skip_step_N no-op that always evaluates to true). The next gate
#   depends on both branches with run_if: AT_LEAST_ONE_SUCCESS, ensuring
#   exactly one path feeds forward and the chain never breaks.
#
# Cluster ID is hardcoded. To change it, search-and-replace the literal value
# across all tasks.
#
# Generated by databricks-job-yaml-gen skill. To regenerate:
#   @databricks-job-yaml-gen <folder_path>
# ============================================================================
```

**Layer 2 — Section headers**:

```yaml
# -- Runtime inputs from ADF -------------------------------------------------
parameters:
  ...

# -- Task graph --------------------------------------------------------------
tasks:
  ...
```

**Layer 3 — STEPID logic comment** (once, above the first `check_step`):

```yaml
# STEPID Pattern: TRUE/FALSE Branch with Skip Tasks
# Each check_step_N evaluates `STEPID <= N`:
#   - true  → StepN notebook runs; skip_step_N is excluded
#   - false → skip_step_N fires (no-op, always true); StepN is excluded
# Next gate depends on both StepN and skip_step_N (outcome: true)
# with run_if: AT_LEAST_ONE_SUCCESS — guaranteeing the chain continues.
```

**Layer 4 — Per-task comments**: explain what the step does and any chaining.

**Layer 5 — Inline comments on `base_parameters`**: mark chained values.

---

### 🔍 Pre-save validation checklist (MANDATORY)

Before writing the YAML to disk, walk through this checklist. Fail fast if any item is violated.

1. **Every `check_step_N` (N > first) has `run_if: AT_LEAST_ONE_SUCCESS`** and depends on
   both `Step<prev>_Task` (no outcome) and `skip_step_<prev>` (outcome: `"true"`).
2. **Every step block has exactly 3 tasks**: `check_step_N`, `StepN_Task`, `skip_step_N`.
3. **Every `skip_step_N`** depends on `check_step_N` outcome `"false"` and uses
   `condition_task` with `op: LESS_THAN_OR_EQUAL`, `left: "1"`, `right: "1"`.
4. **Every notebook task** depends only on `outcome: "true"` of its own `check_step_N`.
5. **No task depends on both `outcome: "true"` AND `outcome: "false"` of the same
   condition task.** This pattern is FORBIDDEN — it causes "Excluded" propagation.
6. Every `condition_task` gate uses `op: LESS_THAN_OR_EQUAL`,
   `left: "{{job.parameters.STEPID}}"`, `right: "<step_id>"` (as a string).
7. Every `base_parameters` key is the **exact** notebook widget name (never renamed).
8. Every value in `base_parameters` is of the form `"{{job.parameters.<name>}}"` and that
   `<name>` exists in the job-level `parameters:` block.
9. No job-level parameter is orphaned (declared but unused) unless explicitly ADF-required.
10. `existing_cluster_id: "<hardcoded_cluster_id>"` appears on every notebook task as a
    **plain string literal** — no `${var.cluster_id}`, no variable references.
11. No `variables:` block exists at the top of the YAML.
12. `STEPID` default is `"1"`; `environment` default is `"dev"`.
13. The last notebook task has `max_retries: 3` and `min_retry_interval_millis: 2000`.
14. Email notifications use the current user's email on both success and failure.
15. The first `check_step` has NO `depends_on` (it is the entry point).

If any check fails → fix before saving. Print the checklist result to the user.

---

### Save location

Write the YAML to:

```
<folder_path>/job.yml
```

Print the absolute path after saving.

---

## STEP 10 — Optional: create the job in the workspace

Ask the user:
> *"Do you want me to create this job in the workspace now via the Jobs API? (yes/no)"*

If yes:
- Convert the YAML to a Jobs API 2.1 payload: strip the `resources: jobs: <name>:` wrapper,
  keep the inner job definition as-is (cluster ID is already a plain literal, so no
  substitution needed)
- Preserve `run_if` on every task that has it
- POST to `/api/2.1/jobs/create`
- Return the `job_id` and a deep link to the job UI

If no: confirm the YAML has been saved and remind the user the cluster ID is hardcoded, so
they can either paste the YAML directly into the Databricks UI or use the Jobs API without
needing `databricks bundle deploy`.

---

## Summary output to user

After generation, print:

1. Resolved notebook folder
2. Notebooks found (with step_ids)
3. Job-level parameters created (with defaults)
4. Parameter chaining detected (if any)
5. Cluster ID used (hardcoded value)
6. Validation checklist result (all ✅ or list failures)
7. Path to saved YAML
8. Next step: paste YAML into Databricks UI, use Jobs API, or run `databricks bundle deploy`
   if you're wrapping this in a bundle
