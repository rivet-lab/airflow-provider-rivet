# Overlap and known limits

How runs of one export are kept apart, and what the package cannot see.

## Overlap

Two runs of the same export must not overlap. rivet's lease (`--lock-wait`, default 600 s: the
second run waits) is not released yet. Until then the builders set `max_active_runs=1` on the
DAG and `max_active_tis_per_dag=1` on every rivet task. **This does not protect against a run
started outside Airflow** (cron, a shell), nor against a second DAG over the same export.

Airflow does not stop two tries of one task from running at once: a zombie try (its worker lost,
the task still running) and its retry, or a rivet left running by a task process that was killed
(the scheduler counts task instances, and the dead try's is already failed). With a state
directory every task but `plan` holds an exclusive lock file while it runs rivet: one per export
for `run`, `apply` and `cdc_run` (a `run` of the whole config takes the lock of every export the config has, in one
sorted order, so it and a run of one of its exports wait for each other); one per table for `load` and `compact` with
`--table`, else one per export. A lock is named by the export, not the config: two configs that share a state directory
and an export name share its lock (they wait for each other; rivet's own lease, keyed by export and source, is the
exact one). The lock's descriptor is passed to every rivet process the task starts, so a rivet that
outlives its task keeps the lock until it exits. A task that finds the lock held waits up to
`lock_wait` seconds, then starts no rivet and ends retryable (`RIVET_AIRFLOW_EXPORT_BUSY`; the
waiting is logged when it starts, `waiting up to <lock_wait> s for the lock of ...`, record `rivet.lock.wait`). The task releases the lock by an explicit unlock once its
rivet has exited.

On Linux each rivet process gets SIGTERM when the task process that started it dies
(`PR_SET_PDEATHSIG`), so a task runner killed outright (OOM, SIGKILL) no longer leaves rivet
running beside its retry. Where that cannot fire (not Linux; the signal is tied to the thread that
started rivet; a rivet that does not stop on SIGTERM) the lock above still keeps a retry from
starting a second rivet until the first has exited.

**Without a state directory (a pod) there is no lock file.** Only rivet's own lease protects the
export: rivet (>= 0.32.2) refuses the second holder with `RIVET_STATE_RUN_IN_PROGRESS` /
`RIVET_LOAD_TABLE_BUSY` (both kind `busy`, exit 2), which the task retries ([Retries](retries.md)); a plain `mode: full` run takes the
run lease too (measured on 0.32.0).

## Known limits

- The Kubernetes mode has unit tests only (no pod was run).
- This package's tests run `load` and `compact` only through the fake binary, which replays
  rivet's output and talks to no warehouse. A live run against BigQuery, Snowflake and ClickHouse
  exists only as the warehouse leg of the live gate (`python -m gate --warehouses ...`, see "Live
  gate"), which reads each warehouse back through a second client and compares it with the source.
- A failure callback runs where Airflow runs task callbacks, in the task's process on the worker;
  exercised live on Airflow 2.10.5 and 3.3.2 by hand (docs/slack.md), not by the live gate.
