---
name: databricks-job-yaml-gen
description: >
  Generates a complete Databricks Workflow YAML definition from a folder of sequenced notebooks.
  Use this skill whenever a user wants to: create a Databricks job from notebooks, generate a
  Databricks Asset Bundle (DAB) YAML, automate an ADF-triggered pipeline, wire up job-level
  parameters for a multi-step notebook workflow, set up conditional step logic (STEPID), or
  chain parameters between pipeline steps. Also triggers when the user mentions phrases like
  "create job YAML", "notebook pipeline config", "Databricks workflow from folder", or
  "ADF-triggered Databricks job". Always use this skill when a Databricks workspace folder
  scan + YAML output is involved — even if the user only says "make a job for this folder".
---

# Databricks Job YAML Generator

Generates a complete, valid Databricks Asset Bundle (DAB) YAML for a sequenced notebook
workflow triggered from Azure Data Factory (ADF), complete with job-level parameters,
per-task base_parameters, conditional STEPID logic, and parameter chaining between steps.

---

## Invocation

```
@databricks-job-yaml-gen /Workspace/Users/<your-path>/<folder-name>
```

The path after the skill name is the **root folder** to scan. Claude will look for `fixed/` inside it automatically.

---

## Overview of the 9-step process

1. Get the current user's email  
2. Scan the folder for a `fixed/` subfolder and list notebooks  
3. Read each notebook and extract parameters + data flow  
4. Define job-level parameters with short-prefix naming  
5. Map job parameters → notebook parameters per task  
6. Chain parameters between dependent steps  
7. Build conditional STEPID logic  
8. Assemble job configuration  
9. Output YAML + summary  

---

## STEP 1 — Get user email

```python
current_user = spark.sql("SELECT current_user()").first()[0]
```

Use this email for `email_notifications.on_success` and `on_failure`.

---

## STEP 2 — Scan the folder

- Look inside the user-specified folder for a subfolder called **`fixed/`**
- If `fixed/` does not exist → ask the user: *"I couldn't find a `fixed/` subfolder in `<folder_path>`. Please provide the path to the folder containing your notebooks."* Then wait for their response and continue from there.
- List all notebooks in `fixed/`
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
| `getArgument("name", "default")` | param name + default |

Use the **exact** param name as written in code.

### b) Source tables

Look for:
- `spark.table(f"{catalog}.{schema}.{table}")`
- `spark.read.table(...)`
- `spark.sql("SELECT * FROM ...")`

Note which parameters feed into the source table reference.

### c) Target tables

Look for:
- `df.write.saveAsTable(...)`
- `df.write.format("delta").save(...)`
- `spark.sql("INSERT INTO ...")`
- `spark.sql("MERGE INTO ...")`

Note which parameters feed into the target table reference.

### d) Data flow

- Which params are **source** (read)?
- Which params are **target** (write)?
- Does this step's source come from the previous step's target?

---

## STEP 4 — Create job-level parameters

ADF passes ALL parameters at job level. Apply this naming convention:

| Purpose | Job param name |
|---|---|
| Step ID | `STEPID` |
| Environment | `environment` |
| 1st source catalog | `s_catalog` |
| 1st source schema | `s_schema` |
| 1st source table | `s_source` |
| 2nd source catalog | `s_catalog2` |
| 2nd source table | `s_source2` |
| Nth source | `s_catalogN`, `s_schemaN`, `s_sourceN` |
| 1st target catalog | `t_catalog` |
| 1st target schema | `t_schema` |
| 1st target table | `t_target` |
| Nth target | `t_catalogN`, `t_schemaN`, `t_targetN` |
| Any other param | **Exact name** from notebook code |

Rules:
- First source → no number suffix (`s_source`, not `s_source1`)
- Collect every unique param across ALL notebooks into one flat list
- Use actual default values from code; use `""` if no default found
- `STEPID` default = `"1"`; `environment` default = `"dev"`

---

## STEP 5 — Map job parameters → notebook parameters

In each task's `base_parameters`, the mapping is:

```
<exact_notebook_param_name>: "{{job.parameters.<job_param_name>}}"
```

### Example: notebook uses standard names

```python
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_catalog", "dev_edf_bronze")
dbutils.widgets.text("source_table", "MY_TABLE")
dbutils.widgets.text("target_catalog", "dev_edf_silver")
dbutils.widgets.text("target_table", "MY_TABLE_CURRENT")
```

→ base_parameters:
```yaml
env: "{{job.parameters.environment}}"
source_catalog: "{{job.parameters.s_catalog}}"
source_table: "{{job.parameters.s_source}}"
target_catalog: "{{job.parameters.t_catalog}}"
target_table: "{{job.parameters.t_target}}"
```

### Example: notebook uses custom names

```python
dbutils.widgets.text("src_cat", "dev_edf_bronze")
dbutils.widgets.text("src_tbl", "MY_TABLE")
dbutils.widgets.text("tgt_tbl", "MY_TABLE_CURRENT")
```

→ base_parameters:
```yaml
src_cat: "{{job.parameters.s_catalog}}"
src_tbl: "{{job.parameters.s_source}}"
tgt_tbl: "{{job.parameters.t_target}}"
```

**KEY: Left side = exact notebook param name. Never rename it.**

---

## STEP 6 — Parameter chaining between steps

If StepN's source is the **same table** Step(N-1) wrote to → point StepN's source params at Step(N-1)'s **target** job params.

```yaml
# Step02 reads what Step01 wrote:
source_catalog: "{{job.parameters.t_catalog}}"
source_schema:  "{{job.parameters.t_schema}}"
source_table:   "{{job.parameters.t_target}}"
```

If Step01 wrote **multiple** targets and Step02 reads them:
```yaml
source_table:  "{{job.parameters.t_target}}"
source_table2: "{{job.parameters.t_target2}}"
```

Only chain when the code **actually** shows this dependency. If Step02 reads a completely independent table, map it to `s_source` / `s_source2` etc.

---

## STEP 7 — Conditional STEPID logic

### Single step → no condition tasks

Just one `notebook_task`. No `depends_on`.

### Multiple steps → condition tasks

For every step **except the last**, create a `condition_task`:

```yaml
condition_task:
  op: LESS_THAN_OR_EQUAL
  left: "{{job.parameters.STEPID}}"
  right: "<step_id>"
```

Dependency wiring rules:

| Task | depends_on |
|---|---|
| `check_step_1` | nothing |
| `check_step_N` | previous `check_step` outcome `"true"` **AND** `"false"` |
| `StepN_Task` (not last) | its own `check_step` outcome `"true"` only |
| Last `StepN_Task` | last `check_step` outcome `"true"` **AND** `"false"` |

The **last notebook task** always runs regardless of STEPID.  
Add `max_retries: 3` and `min_retry_interval_millis: 2000` to the last task.

### Example for steps [1, 3, 7]:

```yaml
tasks:
  - task_key: check_step_1
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "1"

  - task_key: Step01_TaskName
    depends_on:
      - task_key: check_step_1
        outcome: "true"
    notebook_task: ...
    existing_cluster_id: 1015-164218-68x6l8o

  - task_key: check_step_3
    depends_on:
      - task_key: check_step_1
        outcome: "true"
      - task_key: check_step_1
        outcome: "false"
    condition_task:
      op: LESS_THAN_OR_EQUAL
      left: "{{job.parameters.STEPID}}"
      right: "3"

  - task_key: Step03_TaskName
    depends_on:
      - task_key: check_step_3
        outcome: "true"
    notebook_task: ...
    existing_cluster_id: 1015-164218-68x6l8o

  - task_key: Step07_TaskName
    depends_on:
      - task_key: check_step_3
        outcome: "true"
      - task_key: check_step_3
        outcome: "false"
    notebook_task: ...
    existing_cluster_id: 1015-164218-68x6l8o
    max_retries: 3
    min_retry_interval_millis: 2000
```

Execution matrix:
- `STEPID=1` → runs steps 1, 3, 7
- `STEPID=3` → runs steps 3, 7
- `STEPID=7` → runs step 7 only

---

## STEP 8 — Job configuration

```yaml
name: Sqn_<TABLE_NAME>_<folder_name>
email_notifications:
  on_success: [<user_email>]
  on_failure: [<user_email>]
timeout_seconds: 86400
queue:
  enabled: true
existing_cluster_id: 1015-164218-68x6l8o   # same for all tasks
```

Derive `TABLE_NAME` from the main table name found in the notebook names/content.

---

## STEP 9 — Output

### Part 1: YAML

Output ONLY valid YAML under:

```yaml
resources:
  jobs:
    <job_name>:
      ...
```

### Save the YAML file

After printing the YAML, save it to the notebooks folder:

```python
yaml_path = "<fixed_folder_path>/job_config.yml"
dbutils.fs.put(yaml_path, yaml_content, overwrite=True)
print(f"✅ YAML saved to: {yaml_path}")
```

- If `fixed/` was found → save to `<root_folder>/fixed/job_config.yml`
- If user provided a custom path → save to `<user_provided_path>/job_config.yml`
- Confirm the save with the printed path.

### Part 2: Summary (after the YAML)

```
SUMMARY:
a) Job name: <name>
b) Steps found: [list of step_ids]
c) Job-level parameters with defaults:
     STEPID = "1"
     environment = "dev"
     s_catalog = "..."
     s_source = "..."
     t_target = "..."
     ...
d) Parameter mapping per step:
     Step01:
       notebook_param "source_table" → job_param "s_source"
       notebook_param "target_table" → job_param "t_target"
     Step02:
       notebook_param "source_table" → job_param "t_target"  (chained from Step01)
       notebook_param "target_table" → job_param "t_target2"
e) Step execution matrix:
     STEPID=1 → runs [1, 3, 7]
     STEPID=3 → runs [3, 7]
     STEPID=7 → runs [7]
f) Data flow:
     Step01: reads [s_source] → writes [t_target]
     Step02: reads [t_target] → writes [t_target2]
```

---

## Quick-reference checklist

- [ ] Got user email via `spark.sql("SELECT current_user()")`
- [ ] `fixed/` subfolder exists; notebooks listed and sorted
- [ ] All `dbutils.widgets.text/get` and `getArgument` calls extracted
- [ ] Source and target tables identified per notebook
- [ ] Job-level params named with `s_` / `t_` / exact-name convention
- [ ] `base_parameters` use EXACT notebook param names on the left
- [ ] Chained params where step reads previous step's output
- [ ] `condition_task` blocks wired correctly for STEPID logic
- [ ] Last task has `max_retries: 3` + `min_retry_interval_millis: 2000`
- [ ] YAML is valid under `resources.jobs.<job_name>`
- [ ] Summary printed after YAML
