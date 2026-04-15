# LEATHER_SHOP — State-Aware Orchestration with dbt and Snowflake

A hands-on project to learn and experiment with **State-Aware Orchestration (SAO)** on dbt Platform with Fusion, built on top of a realistic Snowflake data pipeline.

This project simulates a French leather goods company with continuously generated data at different cadences, making it the perfect playground to observe how SAO skips unnecessary rebuilds and saves compute.

---
## What is State-Aware Orchestration?

Your dbt jobs run every hour. But most of the time, most of your data hasn't changed. Without SAO, every run rebuilds all models — wasted compute, longer pipelines, and consumers seeing builds with no fresh data.

**State-Aware Orchestration** (available with dbt Fusion) solves this by only building what actually changed. It relies on:

1. **Real-time shared state** across all jobs — if job A already built a model, job B reuses it.
2. **Model-level queueing** — two jobs targeting the same model won't collide.
3. **State-aware AND state-agnostic support** — you can mix dynamic and explicit jobs feeding the same shared state.
4. **Works out-of-the-box** — no mandatory config to get started.

---
## Setup Guide

> **You must complete this step before setting up anything in dbt Platform.**

Connect to your Snowflake account with a user that has the `ACCOUNTADMIN` role and execute the SQL scripts **in order**:

1. **`snowflake/setup_admin.sql`** — Creates warehouses, roles, users, databases, schemas, and grants.
2. **`snowflake/setup_data.sql`** — Creates the raw tables and inserts seed data into both DEV and PRD environments.
3. **`snowflake/setup_generation.sql`** — Creates stored procedures and Snowflake tasks that continuously generate new data.

> **Cost Warning: Suspend tasks when you are done experimenting!**
>
> Running Snowflake tasks consume credits continuously. Each task wakes up the `ORCHESTRATION_WH` warehouse on its schedule (every 1h, 3h, or 6h). If you forget to suspend them, **you will accumulate unnecessary Snowflake costs**.
>
> When you are finished experimenting, always suspend the tasks:
>
> ```sql
> USE ROLE ACCOUNTADMIN;
>
> ALTER TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_DEV_TASK SUSPEND;
> ALTER TASK ORCHESTRATION.JOBS.CUSTOMER_GENERATION_DEV_TASK SUSPEND;
> ALTER TASK ORCHESTRATION.JOBS.PRODUCT_GENERATION_DEV_TASK SUSPEND;
>
> ALTER TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_PRD_TASK SUSPEND;
> ALTER TASK ORCHESTRATION.JOBS.CUSTOMER_GENERATION_PRD_TASK SUSPEND;
> ALTER TASK ORCHESTRATION.JOBS.PRODUCT_GENERATION_PRD_TASK SUSPEND;
> ```
>
> You can check which tasks are currently running with:
> ```sql
> SHOW TASKS IN SCHEMA ORCHESTRATION.JOBS;
> ```

---
### Schedule a dbt Job (Required for SAO)

**State-Aware Orchestration requires a scheduled dbt job running in a deployment environment (Staging or Production).**

1. Go to **Jobs** in dbt Platform.
2. Create a new **Deploy Job** in your Staging or Production environment.
3. Set the schedule to **every 1 hour** (to match the fastest data generation cadence).
4. Use the command: `dbt build`
5. Enable the job.

Without a scheduled job, SAO has no shared state to leverage. The hourly schedule ensures that dbt can detect which sources have changed since the last run and skip models with unchanged upstream data.