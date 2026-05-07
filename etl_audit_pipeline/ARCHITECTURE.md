# ETL Audit Pipeline - Architecture Documentation

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ETL AUDIT PIPELINE SYSTEM                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 1: INITIALIZATION & VALIDATION                                        │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────┐
    │     ETL_AUDT_REF (Reference)         │
    │  ┌────────────────────────────────┐  │
    │  │ job_name                       │  │
    │  │ src_table1                     │  │
    │  │ target_table                   │  │
    │  │ chg_final (CHG/FINAL)          │  │
    │  │ desc_text                      │  │
    │  │ create_ts                      │  │
    │  └────────────────────────────────┘  │
    └──────────────┬───────────────────────┘
                   │
                   │ Lookup & Validate
                   ↓
    ┌──────────────────────────────────────┐
    │      Notebook1: Lookup_Validation    │
    │  ┌────────────────────────────────┐  │
    │  │ 1. Check ETL_AUDT_REF          │  │
    │  │ 2. Validate job config         │  │
    │  │ 3. Get unprocessed items       │  │
    │  │ 4. Select from source table    │  │
    │  │ 5. Insert into ETL_AUDT_FACT   │  │
    │  └────────────────────────────────┘  │
    └──────────────┬───────────────────────┘
                   │
                   │ Create Audit Records
                   ↓

┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 2: AUDIT TRACKING                                                     │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────┐
    │    ETL_AUDT_FACT (Audit Log)         │
    │  ┌────────────────────────────────┐  │
    │  │ audit_id (PK)                  │  │
    │  │ edf_job_run_item_id            │  │
    │  │ processed_flag (Y/N)           │  │
    │  │ run_status                     │  │
    │  │ job_start_time                 │  │
    │  │ job_end_time                   │  │
    │  │ source_count                   │  │
    │  │ target_count                   │  │
    │  │ error_count                    │  │
    │  │ batch_no                       │  │
    │  │ notebook_name                  │  │
    │  └────────────────────────────────┘  │
    └──────────────┬───────────────────────┘
                   │
                   │ Select Batches (processed_flag='N')
                   ↓

┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 3: BATCH PROCESSING & ORCHESTRATION                                   │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────┐
    │  ADF Pipeline / Notebook2 (Loop)     │
    │  ┌────────────────────────────────┐  │
    │  │ 1. Get unprocessed batches     │  │
    │  │ 2. Loop through each batch     │  │
    │  │ 3. Pass batch_no to notebooks  │  │
    │  │ 4. Track execution status      │  │
    │  └────────────────────────────────┘  │
    └──────────────┬───────────────────────┘
                   │
                   │ Branch based on table type
                   │
         ┌─────────┴─────────┐
         │                   │
         ↓                   ↓
    ┌─────────┐         ┌─────────┐
    │   CHG   │         │  FINAL  │
    │  Table  │         │  Table  │
    └────┬────┘         └────┬────┘
         │                   │
         │ Write Mode:       │ Write Mode:
         │ OVERWRITE         │ APPEND
         │                   │
         ↓                   ↓
    ┌─────────────────────────────────┐
    │   Target Tables                 │
    │  ┌───────────────────────────┐  │
    │  │ CHG: Temporary staging    │  │
    │  │      Overwrite each run   │  │
    │  │                           │  │
    │  │ FINAL: Permanent storage  │  │
    │  │        Accumulate data    │  │
    │  └───────────────────────────┘  │
    └─────────────┬───────────────────┘
                  │
                  │ Processing Complete
                  ↓

┌─────────────────────────────────────────────────────────────────────────────┐
│  PHASE 4: FINALIZATION & CLEANUP                                             │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────┐
    │   Final Notebook: Cleanup            │
    │  ┌────────────────────────────────┐  │
    │  │ 1. Update processed_flag='Y'   │  │
    │  │ 2. Set job_end_time            │  │
    │  │ 3. Update run_status           │  │
    │  │ 4. Validate target data        │  │
    │  │ 5. Generate summary            │  │
    │  └────────────────────────────────┘  │
    └──────────────┬───────────────────────┘
                   │
                   │ If CHG table
                   ↓
    ┌──────────────────────────────────────┐
    │   CHG Table Cleanup Notebook         │
    │  ┌────────────────────────────────┐  │
    │  │ 1. Validate CHG table          │  │
    │  │ 2. Get record count            │  │
    │  │ 3. DELETE/TRUNCATE table       │  │
    │  │ 4. Verify cleanup              │  │
    │  │ 5. Prepare for next run        │  │
    │  └────────────────────────────────┘  │
    └──────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  FINAL STATE: READY FOR NEXT RUN                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
┌─────────────┐
│   Source    │
│   Tables    │
└──────┬──────┘
       │
       │ Extract
       ↓
┌─────────────┐      ┌──────────────┐
│  Staging    │─────→│ ETL_AUDT_REF │ (Lookup)
│   Layer     │      └──────────────┘
└──────┬──────┘
       │
       │ Validate & Track
       ↓
┌─────────────┐
│ETL_AUDT_FACT│ (Create audit records)
└──────┬──────┘
       │
       │ Process in Batches
       ↓
┌─────────────┐
│ Processing  │
│   Logic     │
└──────┬──────┘
       │
       ├─────────────┬─────────────┐
       │             │             │
       ↓             ↓             ↓
┌──────────┐  ┌──────────┐  ┌──────────┐
│CHG Table │  │CHG Table │  │  FINAL   │
│(Overwrite│  │(Overwrite│  │  Table   │
│   Mode)  │  │   Mode)  │  │ (Append) │
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │
     │ Clean up    │ Clean up    │ Persist
     ↓             ↓             ↓
┌─────────────────────────────────┐
│      Ready for Next Run         │
└─────────────────────────────────┘
```

## Component Interaction

```
┌────────────────────────────────────────────────────────────────┐
│                    Azure Data Factory                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  Pipeline Orchestration                   │  │
│  │                                                            │  │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐           │  │
│  │  │Notebook1 │───→│Notebook2 │───→│  Final   │           │  │
│  │  │Validation│    │Processing│    │ Cleanup  │           │  │
│  │  └──────────┘    └──────────┘    └──────────┘           │  │
│  │       │               │                │                  │  │
│  └───────┼───────────────┼────────────────┼─────────────────┘  │
│          │               │                │                    │
└──────────┼───────────────┼────────────────┼────────────────────┘
           │               │                │
           ↓               ↓                ↓
┌────────────────────────────────────────────────────────────────┐
│                    Azure Databricks                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  Spark Cluster                            │  │
│  │                                                            │  │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐           │  │
│  │  │ PySpark  │    │ PySpark  │    │ PySpark  │           │  │
│  │  │ Notebook │    │ Notebook │    │ Notebook │           │  │
│  │  └──────────┘    └──────────┘    └──────────┘           │  │
│  │       │               │                │                  │  │
│  └───────┼───────────────┼────────────────┼─────────────────┘  │
│          │               │                │                    │
└──────────┼───────────────┼────────────────┼────────────────────┘
           │               │                │
           ↓               ↓                ↓
┌────────────────────────────────────────────────────────────────┐
│                    Database Layer                              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │  │
│  │  │ETL_AUDT_REF  │  │ETL_AUDT_FACT │  │Target Tables │   │  │
│  │  │  (Control)   │  │  (Audit Log) │  │ (CHG/FINAL)  │   │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

## State Transition Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Job Execution States                         │
└─────────────────────────────────────────────────────────────────┘

    [START]
       │
       ↓
┌──────────────┐
│  NOT_FOUND   │ ← Job not in ETL_AUDT_REF
└──────────────┘
       │
       │ Config Found
       ↓
┌──────────────┐
│  VALIDATED   │ ← Job config validated
└──────────────┘
       │
       │ Create Audit Record
       ↓
┌──────────────┐
│ PENDING (N)  │ ← processed_flag='N'
└──────────────┘
       │
       │ Start Processing
       ↓
┌──────────────┐
│  PROCESSING  │ ← run_status='RUNNING'
└──────────────┘
       │
       ├─────────────┬─────────────┐
       │             │             │
       ↓             ↓             ↓
┌──────────┐  ┌──────────┐  ┌──────────┐
│ SUCCESS  │  │  FAILED  │  │ PARTIAL  │
└──────────┘  └──────────┘  └──────────┘
       │             │             │
       └─────────────┴─────────────┘
                     │
                     ↓
              ┌──────────────┐
              │COMPLETED (Y) │ ← processed_flag='Y'
              └──────────────┘
                     │
                     ↓
              ┌──────────────┐
              │ CHG Cleanup  │ (if CHG table)
              └──────────────┘
                     │
                     ↓
                  [END]
```

## Error Handling Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Error Handling Strategy                      │
└─────────────────────────────────────────────────────────────────┘

    [Error Occurs]
         │
         ↓
    ┌─────────┐
    │ Capture │
    │  Error  │
    └────┬────┘
         │
         ├──→ Log to ETL_AUDT_FACT
         │    - error_count++
         │    - error_message
         │    - run_status='FAILED'
         │
         ├──→ ADF Retry Logic
         │    - Retry 2 times
         │    - 30 sec interval
         │
         └──→ Notification
              - Email alert
              - Dashboard update
              - Ticket creation

    [Recovery Options]
         │
         ├──→ Manual Intervention
         │    - Fix data issue
         │    - Reset processed_flag
         │    - Re-run pipeline
         │
         └──→ Automatic Retry
              - Next scheduled run
              - Pick up failed batches
              - Continue processing
```

## Performance Optimization

```
┌─────────────────────────────────────────────────────────────────┐
│                  Performance Considerations                     │
└─────────────────────────────────────────────────────────────────┘

1. Batch Processing
   ┌────────────────────────────────────┐
   │ Large Dataset                      │
   └────────┬───────────────────────────┘
            │
            │ Split into batches
            ↓
   ┌────────────────────────────────────┐
   │ Batch 1 │ Batch 2 │ ... │ Batch N  │
   └────────────────────────────────────┘
            │
            │ Process in parallel
            ↓
   ┌────────────────────────────────────┐
   │ Faster completion time             │
   └────────────────────────────────────┘

2. Indexing Strategy
   - Index on job_run_item_id
   - Index on processed_flag
   - Index on batch_no
   - Composite indexes for common queries

3. Partition Strategy
   - Partition ETL_AUDT_FACT by date
   - Partition target tables by batch_no
   - Optimize for query patterns

4. Caching
   - Cache ETL_AUDT_REF (small, frequently accessed)
   - Cache batch metadata
   - Use Spark broadcast for small lookups
```

## Security Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Security Layers                              │
└─────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────┐
│  Authentication & Authorization        │
│  - Azure AD Integration                │
│  - Service Principal                   │
│  - Managed Identity                    │
└────────────┬───────────────────────────┘
             │
             ↓
┌────────────────────────────────────────┐
│  Network Security                      │
│  - VNet Integration                    │
│  - Private Endpoints                   │
│  - Firewall Rules                      │
└────────────┬───────────────────────────┘
             │
             ↓
┌────────────────────────────────────────┐
│  Data Security                         │
│  - Encryption at Rest                  │
│  - Encryption in Transit               │
│  - Column-level Security               │
└────────────┬───────────────────────────┘
             │
             ↓
┌────────────────────────────────────────┐
│  Audit & Compliance                    │
│  - Activity Logging                    │
│  - Access Tracking                     │
│  - Compliance Reports                  │
└────────────────────────────────────────┘
```

## Scalability Model

```
┌─────────────────────────────────────────────────────────────────┐
│                    Scalability Approach                         │
└─────────────────────────────────────────────────────────────────┘

Horizontal Scaling:
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Worker 1 │  │ Worker 2 │  │ Worker 3 │  │ Worker N │
└────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     │             │             │             │
     └─────────────┴─────────────┴─────────────┘
                   │
                   ↓
            ┌──────────────┐
            │ Shared State │
            │(ETL_AUDT_*)  │
            └──────────────┘

Vertical Scaling:
- Increase Databricks cluster size
- Add more cores/memory
- Use larger VM SKUs
- Optimize Spark configurations
```

---

**Document Version:** 1.0  
**Last Updated:** 2026-05-07  
**Maintained By:** Data Engineering Team