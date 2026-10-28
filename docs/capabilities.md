# Capabilities

What the package does today when the rivet it runs (0.32.2 or newer) lacks a member of the scheduler contract.

## Works today / needs a newer rivet

ADR-0039 describes several things no released binary emits yet. The package reads what rivet's
output holds and takes a recorded fallback for the rest: every task's XCom carries
`degraded: [<key>, ...]`; the task log names each key in words, and the `rivet.result` record at
DEBUG carries the list. Nothing is guessed without a key.

**How it detects.** An output member (`class`, `failures[]`, `stop_reason`) is detected by its
presence in the output itself. The version is used for one thing only: the minimum-version refusal
(rivet 0.32.2; every flag the package passes exists on every subcommand it passes it to there, so
no flag is probed).

"Needs" names the ADR-0039 section; none of it is in a released rivet at this package's date.

| Key | Missing in rivet 0.32.2 | What the package does today | Needs (ADR-0039) |
|---|---|---|---|
| `error_object` | `class`, `retryable`, `kind`, `action` on the `--json-errors` line | derives `class` and `retryable` from the printed line's integer `exit_class`; `kind` and `action` are null | D1/D2, rivet >= unreleased |
| `per_unit_error` | `failures[]` and `per_export[].error` | when several units of one process failed, each carries the process-level object | D2/D3, rivet >= unreleased |
| `stop_reason` | CDC `stop_reason` and `tables[]` | `stop_reason` is null: a `max_events` stop cannot be told from `caught_up` | D4, rivet >= unreleased |

Five more keys record something the worker or one output lacks, not the binary's version:

| Key | What is missing | What the package does |
|---|---|---|
| `stderr_temp` | a state directory or `stderr_dir` on the worker | writes rivet's stdout and stderr to a private per-user directory under the temp directory (see Secrets); a pod loses them with its disk |
| `crashed_ledger` | a readable and writable crashed-budget ledger | without a state directory: a crash is retried only within the first `crashed_retries` tries of the task instance; with a ledger that cannot be read or written: no crash retry at all |
| `result_malformed` | an error object or a result entry with the contract's keys and types; also a stderr line that starts with `{` and is not JSON, or a summary file of an exit-0 process that is not a JSON object | a malformed error object is ignored and the task is classified by the exit-code table; a value of the wrong type is null; a status outside the contract's words counts as `failed` |
| `result_missing` | the `--summary-output` file of a process that exited 0 | rivet could not write it (it only warns); the units are built as if the binary had no summary flag: one unit per requested export or table, status from the exit |
| `exit_zero_failed_unit` | a non-zero exit when the summary reports a failed unit | the task fails, classified by the worst failed unit (`data_integrity` > `internal` > `refusal` > `schema_drift` > `retryable` > `generic`, ADR-0039 D2) |

Not detectable, so not recorded per task:

- A child process killed by a signal is not reported as `crashed` by today's binary: the parent
  exits 1 (or 2, by a text match) and the task follows that exit. A kill of the rivet process
  the operator started is seen directly and is `crashed` today.
- There is no `RIVET_STATE_FOREIGN` refusal yet. The package's own marker check (below) is the
  only guard against a wrong state directory.
- rivet does not yet warn about a relative `cdc.checkpoint` under PostgreSQL state.

The list lives in one place in the code, `airflow_provider_rivet/capabilities.py`
(`DEGRADATIONS`), and a test fails when a key has no row in this table.
