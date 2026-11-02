"""Every source leg grades each rivet task's XCom unit and task log against what the cycle did to the source.

MySQL graded its tasks from the start; SQL Server, MongoDB and Oracle were skipped (the legs had no hand-kept counts for
them). The stand here answers as a faithful rivet would, with row counts written out in this file (not computed by the
code under test), so a count the leg expects wrongly, or a task it does not grade, turns a cell RED.

Each test names the mutant it is RED on.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from test_conns import CLOUD
from test_source_dags import fixture, leg_with
from test_sources_mongodb_oracle import MONGO_BATCH_REFUSAL
from test_stand_hygiene import laid_out

from gate import cycle_effect, sources
from gate.scenario import Verdicts
from gate.source_dags import KINDS
from gate.sources import MONGODB, MSSQL, MYSQL, ORACLE, SourceLeg

ENGINES = [MYSQL, MSSQL, MONGODB, ORACLE]
CYCLES = ("initial", "delta", "idle")
FIXTURES = Path(__file__).parent / "fixtures"
#: The rows each cycle's tasks move for (customers, orders), as the scenario's script makes them: the baseline (all rows)
#: on the first cycle; then customers 20 inserted + 10 updated, orders 100 inserted + 30 updated + 5 deleted. A CDC
#: stream carries every change event (the delete too); an incremental batch export the rows whose cursor moved.
CDC_ROWS = {"initial": (1000, 5000), "delta": (30, 135), "idle": (0, 0)}
BATCH_ROWS = {"initial": (1000, 5000), "delta": (30, 130), "idle": (0, 0)}
#: SQL Server's DATETIME2(7) cursor: rivet keeps its high-water to the microsecond, so the initial rows of the mark's
#: microsecond are read again by the delta's extracts (990 customers, 4965 orders: the live battery's counts).
BATCH_TIES = {"mssql": {"delta": (990, 4965)}}


def batch_expected(engine: sources.Engine, cycle: str) -> tuple[int, int]:
    """The rows each table's incremental extract reads in `cycle`, at the cursor's precision (the model of the count)."""
    ties = BATCH_TIES.get(engine.name, {}).get(cycle, (0, 0))
    return tuple(a + b for a, b in zip(BATCH_ROWS[cycle], ties, strict=True))


TASK = re.compile(r"^(?:(?P<group>.+)\.)?(?P<verb>run|load|compact|apply)(?:\.(?P<unit>.+))?$")


def source_at(engine: sources.Engine, cycle: str) -> dict[str, list[dict[str, Any]]]:
    """The source the gate reads after `cycle`: the rows the delta script wrote carry the newer cursor (2)."""
    grown = cycle != "initial"
    out = {}
    for table, size, moved in zip(engine.tables, (1000 + 20 * grown, 5000 + 95 * grown), (30, 130), strict=True):
        out[table] = [{"updated_at": 2 if grown and i < moved else 1} for i in range(size)]
    return out


def faithful(leg: SourceLeg, dag: str, cycle: str) -> dict[str, int | None]:
    """What each task of `dag` reports in `cycle`: a run, the rows of its stream's tables; a load or an extract, its
    table's; a compaction no count."""
    per = dict(
        zip(
            ("customers", "orders"),
            batch_expected(leg.engine, cycle) if dag.endswith("_batch") else CDC_ROWS[cycle],
            strict=True,
        )
    )
    out: dict[str, int | None] = {}
    for task in leg.tasks[dag]:
        m = TASK.match(task)
        assert m, task
        if m["verb"] == "compact":
            out[task] = None
            continue
        stream = [t for t in leg.tasks[dag] if TASK.match(t)["group"] == m["group"]]
        if m["verb"] == "run":
            out[task] = sum(per[tb] for tb in {tb for t in stream for tb in per if tb in t.lower()})
        else:
            out[task] = per[next(tb for tb in per if tb in m["unit"].lower())]
    return out


def xcom(rows: int | None) -> dict[str, Any]:
    status = "compacted" if rows is None else "success"
    unit = {"export": "e", "table": None, "status": status, "run_id": "r", "rows": rows, "files": 1, "error": None}
    return {"command": "run", "decision": "success", "units": [unit], "error": None, "exit_status": 0}


def log(task: str, rows: int | None) -> str:
    return f"rivet {task} → OK: {'compacted' if rows is None else f'{rows} rows, 1 file'} in 1.0 s (run r)\n"


class Rivet:
    """The stand's answers for one cycle: every task succeeded, its XCom unit and log say `rows`."""

    def __init__(self, leg: SourceLeg, cycle: str, wrong: dict[tuple[str, str], int] | None = None) -> None:
        self.dir = Path("/nonexistent")
        self.state_place = "gate"
        self.leg = leg
        self.moved = {
            (dag, task): rows if (dag, task) not in (wrong or {}) or rows is None else rows + wrong[(dag, task)]
            for dag in leg.dags
            for task, rows in faithful(leg, dag, cycle).items()
        }

    def tasks(self, dag: str, run_id: str) -> dict[str, dict[str, str]]:
        return {t: {"state": "success"} for t in self.leg.tasks[dag]}

    def xcom(self, dag: str, run_id: str, task: str) -> dict[str, Any]:
        return xcom(self.moved[(dag, task)])


class Done:
    def __init__(self, code: int = 0, said: str = "") -> None:
        self.returncode, self.stdout, self.stderr = code, said, ""


def prepared(engine: sources.Engine, tmp_path: Path, monkeypatch) -> SourceLeg:
    """`SourceLeg.prepare` against init's recorded output for the engine, the stack's commands stubbed."""
    stand = laid_out(tmp_path, sources=(engine,))
    leg = SourceLeg(CLOUD, "3.3.2", engine)

    def compose(*args, extra_env=None, check=True, **kw):
        if "airflow-worker" in args:
            kind = "cdc" if args[args.index("--mode") + 1] == "cdc" else "batch"
            if engine is MONGODB and kind == "batch":
                return Done(1, MONGO_BATCH_REFUSAL)
            out = Path(str(stand.rivet_dir) + args[-1].removeprefix("/opt/rivet"))
            out.write_text((FIXTURES / f"init_{engine.name}_{kind}.yaml").read_text())
        return Done()

    monkeypatch.setattr(stand, "compose", compose)
    monkeypatch.setattr(stand, "psql", lambda *a: "")
    monkeypatch.setattr(SourceLeg, "wait_ready", lambda self, stand: None)
    monkeypatch.setattr(SourceLeg, "sql", lambda self, stand, text: "")
    monkeypatch.setattr(sources, "sh", lambda *a, **k: None)
    monkeypatch.setattr(sources.warehouse, "_bq", lambda *a, **k: None)
    leg.prepare(stand)
    return leg


def played(leg: SourceLeg, monkeypatch, wrong: dict[str, dict[tuple[str, str], int]] | None = None) -> tuple[list, list]:
    """The three cycles through `SourceLeg.grade` (the warehouse graders stubbed): the verdict rows and the staged-part
    checks asked for. `wrong`: per cycle, the rows a task reports beyond what a faithful rivet does."""
    v, asked, now = Verdicts("3.3.2"), [], {}
    leg.stale_copy = None  # MySQL's git-sync cell is its own test's
    leg.high_water = {}  # the leg's memory of the run before
    monkeypatch.setattr(sources, "task_log", lambda stand, dag, run_id, task, attempt=None: now["rivet"].log(dag, task))
    monkeypatch.setattr(leg, "source_rows", lambda stand, added=None: source_at(leg.engine, now["cycle"]), raising=False)
    monkeypatch.setattr(leg, "grade_targets", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(
        leg,
        "past_rows",
        lambda stand: dict(zip(leg.engine.tables, batch_expected(leg.engine, now["cycle"]), strict=True)),
        raising=False,
    )
    monkeypatch.setattr(leg, "grade_staged", lambda v, name, kinds: asked.append((name, tuple(kinds))), raising=False)
    for cycle in CYCLES:
        rivet = Rivet(leg, cycle, (wrong or {}).get(cycle))
        rivet.log = lambda dag, task, rivet=rivet: log(task, rivet.moved[(dag, task)])
        now.update(cycle=cycle, rivet=rivet)
        leg.grade(rivet, v, cycle, "r1")
    return v.rows, asked


def failed(rows: list) -> list[str]:
    return sorted(r["cell"] for r in rows if r["status"] == "FAIL")


def test_the_batch_expectation_is_counted_past_the_mark_at_the_cursors_precision(tmp_path: Path, monkeypatch) -> None:
    leg = prepared(MSSQL, tmp_path, monkeypatch)
    texts: list[str] = []

    def count(self, stand, text):
        texts.append(text)
        return "  7\n"

    monkeypatch.setattr(SourceLeg, "sql", count)
    leg.high_water = {"gate_customers": datetime(2026, 10, 28, 12, 0, 0, 123456), "gate_orders": None}
    # Mutant: the rows past the mark counted in Python, to the microsecond (the source's values are cut to it).
    assert leg.past_rows(None) == {"gate_customers": 7, "gate_orders": 7}
    marked, first = texts
    assert marked.endswith("WHERE updated_at > CAST('2026-10-28 12:00:00.123456' AS datetime2(7));"), marked
    assert "WHERE" in marked and "WHERE" not in first


@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.name)
def test_every_engine_grades_every_rivet_task_of_every_cycle(engine, tmp_path: Path, monkeypatch) -> None:
    leg = prepared(engine, tmp_path, monkeypatch)
    rows, asked = played(leg, monkeypatch)
    assert failed(rows) == [], failed(rows)
    cells = {r["cell"] for r in rows if r["status"] == "PASS"}
    # Mutant: the grading kept to the engine whose changes had hand-written counts (MySQL): no cell for the others.
    want = {
        f"{cycle}/{dag}/{task}/{kind}"
        for cycle in CYCLES
        for dag in leg.dags
        for task in leg.tasks[dag]
        for kind in ("xcom-unit", "task-log")
    }
    assert want <= cells, sorted(want - cells)
    # Mutant: the load block / the staged parts left ungraded for the engine, or the high-water never taken.
    assert [c for c in sorted(cells) if c.endswith("/load-block-as-written")] == [
        f"initial/{engine.name}/{kind}/load-block-as-written" for kind in sorted(leg.kinds)
    ]
    assert asked == [(cycle, tuple(leg.kinds)) for cycle in CYCLES]
    assert set(leg.high_water) == set(engine.tables)


@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.name)
def test_a_wrong_row_count_in_any_task_is_that_tasks_red_cell(engine, tmp_path: Path, monkeypatch) -> None:
    leg = prepared(engine, tmp_path, monkeypatch)
    counted = [
        (cycle, dag, task)
        for cycle in CYCLES
        for dag in leg.dags
        for task, rows in faithful(leg, dag, cycle).items()
        if rows is not None
    ]
    assert counted, "a leg with no counted task grades nothing"
    for cycle, dag, task in counted:
        # Mutant: the task's expectation None (not counted), or computed from another table / another cycle.
        for off in (1, -1):
            rows, _ = played(leg, monkeypatch, {cycle: {(dag, task): off}})
            if cycle == "idle" and off == -1:
                continue  # no task reports fewer than none
            assert failed(rows) == [f"{cycle}/{dag}/{task}/xcom-unit"], (cycle, dag, task, off, failed(rows))


#: The CDC tasks that read the orders table: SQL Server and MongoDB capture a table per stream (a DAG each), MySQL one stream.
ORDERS_CDC = {
    "mysql": ("gate_src_mysql_cdc", "load.gate_orders"),
    "mssql": ("gate_src_mssql_cdc_gate_orders_cdc", "load.dbo.gate_orders"),
    "mongodb": ("gate_src_mongodb_cdc_gate_orders_cdc", "load.gate_orders"),
}


@pytest.mark.parametrize("engine", [MYSQL, MSSQL, MONGODB], ids=lambda e: e.name)
def test_a_change_count_the_script_does_not_make_turns_the_cdc_cells_of_the_delta_red(engine, tmp_path, monkeypatch) -> None:
    leg = prepared(engine, tmp_path, monkeypatch)
    # Mutant: one count of `cycle_effect.DELTA` off by one (the expectation and the script both read it, rivet does not).
    monkeypatch.setitem(cycle_effect.DELTA, "gate_orders", {**cycle_effect.DELTA["gate_orders"], "update": 31})
    rows, _ = played(leg, monkeypatch)
    dag, load = ORDERS_CDC[engine.name]
    assert failed(rows) == sorted([f"delta/{dag}/run/xcom-unit", f"delta/{dag}/{load}/xcom-unit"])


def test_oracle_batch_is_graded_on_the_rows_past_the_high_water_it_took_from_the_source(tmp_path, monkeypatch) -> None:
    leg = prepared(ORACLE, tmp_path, monkeypatch)
    assert leg.dags == ["gate_src_oracle_batch"] and "cdc" not in leg.texts, "Oracle CDC is a known limit: no stream"
    rows, _ = played(leg, monkeypatch)
    cells = {r["cell"] for r in rows if r["status"] == "PASS"}
    # Mutant: the high-water not taken after a cycle (the delta reads every row again), or `>` -> `>=`.
    assert "delta/gate_src_oracle_batch/load.GATE_ORDERS/xcom-unit" in cells
    assert "idle/gate_src_oracle_batch/apply.GATE_ORDERS/xcom-unit" in cells


def dags_graded(engine: sources.Engine, monkeypatch) -> tuple[list, SourceLeg]:
    """The three cycles' `grade_tasks` on a `--source-dags` leg of the engine (init's recorded configs, the stand faithful)."""
    leg = leg_with(engine, {k: fixture(f"init_{engine.name}_{k}.yaml") for k in KINDS[engine.name]})
    v = Verdicts("3.3.2")
    for cycle in CYCLES:
        rivet = Rivet(leg, cycle)
        leg.high_water = {t: {"initial": None, "delta": 1, "idle": 2}[cycle] for t in engine.tables}
        leg.past = dict(zip(engine.tables, batch_expected(engine, cycle), strict=True))
        monkeypatch.setattr(
            sources, "task_log", lambda stand, dag, run_id, task, attempt=None, r=rivet: log(task, r.moved[(dag, task)])
        )
        leg.grade_tasks(
            rivet, v, cycle, "r1", leg.dags, source_at(engine, cycle), cycle_effect.ops(cycle), baseline=cycle == "initial"
        )
    return v.rows, leg


# What `--source-dags` builds: one DAG per config, a group per stream (named after its export), the loads named by the
# warehouse table (`dbo_gate_orders`).
@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.name)
def test_source_dags_grade_the_tasks_of_each_stream_group(engine, monkeypatch) -> None:
    rows, leg = dags_graded(engine, monkeypatch)
    assert failed(rows) == [], failed(rows)
    graded = {r["cell"] for r in rows}
    # Mutant: a group-prefixed task id not understood (the unit read as the group), or a stream's tables not scoped.
    want = {f"{cycle}/{dag}/{task}/xcom-unit" for cycle in CYCLES for dag in leg.dags for task in leg.tasks[dag]}
    assert want <= graded, sorted(want - graded)


#: The CDC tasks that read the customers table in a `--source-dags` DAG (Oracle's stream is files only, graded on its run).
CUSTOMERS_CDC = {
    "mysql": ("rivet_mysql_cdc_cdc", ["run", "load.gate_customers"]),
    "mssql": ("rivet_mssql_cdc_cdc", ["gate_customers_cdc.run", "gate_customers_cdc.load.dbo_gate_customers"]),
    "mongodb": ("rivet_mongodb_cdc_cdc", ["gate_customers_cdc.run", "gate_customers_cdc.load.gate_customers"]),
    "oracle": ("rivet_oracle_cdc_cdc", ["GATE_CUSTOMERS_cdc.run"]),
}


@pytest.mark.parametrize("engine", ENGINES, ids=lambda e: e.name)
def test_source_dags_expect_the_change_counts_of_the_script_in_every_engines_stream(engine, monkeypatch) -> None:
    # Mutant: one count of `cycle_effect.DELTA` off by one (here customers' updates), or the ops looked up by the
    # table's case as the engine names it (Oracle's upper-case keys find none: the run expects only the baseline).
    monkeypatch.setitem(cycle_effect.DELTA, "gate_customers", {**cycle_effect.DELTA["gate_customers"], "update": 11})
    rows, _ = dags_graded(engine, monkeypatch)
    dag, tasks = CUSTOMERS_CDC[engine.name]
    assert failed(rows) == sorted(f"delta/{dag}/{t}/xcom-unit" for t in tasks)


def test_a_unit_no_stream_table_is_named_by_is_a_red_cell_not_a_crash() -> None:
    leg = leg_with(MSSQL, {k: fixture(f"init_mssql_{k}.yaml") for k in KINDS["mssql"]})
    dag = leg.source_dags["cdc"]
    leg.tasks[dag] = [*leg.tasks[dag], "gate_orders_cdc.load.dbo_invoices"]
    rivet = SimpleNamespace(
        tasks=lambda d, r: {t: {"state": "success"} for t in leg.tasks[d]},
        xcom=lambda d, r, t: xcom(7),
        dir=Path("/nonexistent"),
    )
    v = Verdicts("3.3.2")
    leg.grade_tasks(rivet, v, "delta", "r1", [dag], source_at(MSSQL, "delta"), cycle_effect.ops("delta"), baseline=False)
    odd = [r for r in v.rows if r["cell"] == f"delta/{dag}/gate_orders_cdc.load.dbo_invoices/xcom-unit"]
    # Mutant: an unknown unit expected as some other table's rows (or the lookup's error left to crash the leg).
    assert [r["status"] for r in odd] == ["FAIL"] and "dbo_invoices" in odd[0]["detail"]
