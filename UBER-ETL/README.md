# uber-taxi-agentic-etl

Capstone Project 3: an ETL pipeline for Uber ride-booking data built with PySpark on Azure Databricks, with data
stored in ADLS Gen2 (medallion layers: Bronze, Silver, Gold). A LangGraph multi-agent system, powered by a GPT model
on Microsoft Foundry, runs, validates and reports on the pipeline.

## Repository structure

```
uber-taxi-agentic-etl/
  data/
    sample_rides.csv        152-row sample of the source file (clean)
    corrupted_rides.csv     the same rows with deliberate faults (for tests)
    schema.json             the 21-column source schema
  notebooks/
    setup.ipynb             Spark session, ADLS access, containers, schema, connectivity, Foundry test
    profiling.ipynb         data profiling
    test_pipeline.ipynb     Phase 1 unit tests + end-to-end check of the full tables
    run_agents.ipynb        Phase 2: agent runs, failure scenarios, evaluation metrics
  src/
    config.py               paths, storage account, thresholds, model settings
    etl_pipeline.py         Raw -> Bronze -> Silver (+ quarantine)
    gold_analytics.py       Silver -> Gold tables
    quality_checks.py       data quality rules returning structured pass/fail results
    agents.py               LangGraph supervisor + etl, quality, analytics, explain agents, tools, Foundry client
  requirements.txt
  .env.example
```

## Setup

1. **Azure:** create resource group `rg-uber-etl`, and a storage account `stuberetlabhi01` with hierarchical
   namespace (ADLS Gen2). Create the containers `raw`, `bronze`, `silver`, `gold`, `quarantine` and `reports`,
   then upload the CSV to `raw/uber_rides/`.
2. **Service principal:** create `sp-uber-etl` with a client secret, and give it the role
   **Storage Blob Data Contributor** on the storage account.
3. **Key Vault and secret scope:** store `sp-client-id`, `sp-tenant-id` and `sp-client-secret` in Key Vault
   `kv-uber-etl-abhi01`. Create the Key Vault-backed Databricks secret scope `uber-etl-scope`.
4. **Databricks:** in workspace `dbw-uber-etl`, create cluster `uber-etl-cluster` (single node, LTS runtime,
   Dedicated access mode). Install these libraries from PyPI: `azure-identity`, `azure-storage-file-datalake`,
   `databricks-sdk`, `openai`, `langgraph`.
5. **Git folder:** link this repository as a Databricks Git folder, then run `notebooks/setup.ipynb`
   (except its last cell, which needs Phase 2 setup).

No secrets are in code: `config.configure_adls(spark)` reads them from the secret scope.

## Execution order

| Step | What | Result |
|---|---|---|
| 1 | `notebooks/setup.ipynb` | ADLS reachable, containers and schema ready |
| 2 | `notebooks/profiling.ipynb` | data profiled |
| 3 | `notebooks/test_pipeline.ipynb` (all cells except the last) | unit tests pass on clean data and fail correctly on corrupted data |
| 4 | Create the Databricks Job (below) | Job `uber-taxi-etl` |
| 5 | **Run now** on the Job | full pipeline on the full CSV |
| 6 | Last cell of `test_pipeline.ipynb` | all checks pass on the full tables (end of Phase 1) |
| 7 | Phase 2 setup (below), then the last cell of `setup.ipynb` | Foundry GPT reachable |
| 8 | `notebooks/run_agents.ipynb` | agent runs, failure scenarios, evaluation metrics, reports in `reports/runs/` |

### Databricks Job `uber-taxi-etl`

Job parameters: `mode` = `full`, `start_date` = (empty), `end_date` = (empty).

| Task | Type | File | Parameters | Depends on |
|---|---|---|---|---|
| `bronze` | Python script | `src/etl_pipeline.py` | `["--layer","bronze","--mode","{{job.parameters.mode}}","--start-date","{{job.parameters.start_date}}","--end-date","{{job.parameters.end_date}}"]` | none |
| `silver` | Python script | `src/etl_pipeline.py` | the same, with `silver` | bronze |
| `quality` | Python script | `src/quality_checks.py` | `["--layers","bronze,silver"]` | silver |
| `gold` | Python script | `src/gold_analytics.py` | `[]` | quality |

Every task runs on `uber-etl-cluster` with **Retries: 1**. Under Job notifications, add an e-mail alert **on failure**.
The `quality` task fails when any hard rule fails, which stops `gold` and sends the alert.
The `etl` agent runs only the `bronze` and `silver` tasks.

### Phase 2 setup (Microsoft Foundry)

1. Deploy a GPT model in Microsoft Foundry. Note the endpoint and the deployment name.
2. On the Foundry resource, under **Access control (IAM)**, give `sp-uber-etl` the role **Cognitive Services OpenAI User**.
   This enables Microsoft Entra ID authentication, so no API key is needed.
3. In Key Vault, add the secrets `foundry-endpoint` and `foundry-deployment`.

## Profiling findings

* 150,000 rows and 21 columns, covering every day from 2024-01-01 to 2024-12-30.
* The Date column is `yyyy-MM-dd` in the file, not `dd-MM-yyyy` as the specification states. The parser accepts both.
* There are 5 Booking Status values: Completed, Cancelled by Driver, Cancelled by Customer, No Driver Found and Incomplete.
* VTAT (vehicle turnaround time) is present except for No Driver Found. CTAT (customer turnaround time) is present only
  for Completed and Incomplete rides.
* 1,224 Booking IDs appear more than once (1,233 extra rows). Silver keeps the earliest and quarantines the others.

## Data quality rules

The rules are in `src/quality_checks.py`, and their thresholds and hard/soft flags are in `config.QUALITY_RULES`:
completeness, outcome completeness, uniqueness, validity, consistency, timeliness, volume versus the previous load,
Bronze-to-Silver reconciliation, and Gold checks (totals reconcile with Silver, no missing dates, rates plausible
versus history). A failed hard rule stops the run. The LLM can never override it.

## Agents

| Node | Role | Tools |
|---|---|---|
| supervisor | GPT routes the request to `etl`, `quality`, `analytics` or `explain` | Foundry GPT client |
| etl | runs the Job (Bronze and Silver), polls it, collects output; ends the run on failure | `run_etl_job`, `get_run_status` |
| quality | runs the Bronze and Silver checks; GPT classifies severity; ends the run if a hard rule fails | `run_quality_checks`, `compare_row_counts` |
| analytics | builds the Gold tables and validates them | `gold_analytics.py`, `run_quality_checks` |
| explain | answers questions from run reports and table stats; writes the run report to `reports/runs/` | `get_table_stats`, `write_report` |

**Guardrails:**
* Agents can use only the approved tools and can write only to `reports/runs/`.
* A full reload (overwrite), or a backfill over 31 days, needs human approval.
* Booking and Customer IDs are masked in prompts.
* Every tool call is logged in the run report.
* Token use, retries, graph steps and job time are capped.
