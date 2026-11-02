"""What each gate cycle does to a source and the change counts it causes: the one place both are written.

The scripts are rendered from `DELTA`, so a count appears once; every leg reads its expected operations, the keys a batch
target keeps and the engine's script from here."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

#: The rows the scenario seeds into `gate_orders` (the delete takes the top ones, the inserts continue after them).
SEED_ORDERS = 5000
#: The change set the delta script makes on every engine, per table and operation.
DELTA: dict[str, dict[str, int]] = {
    "gate_customers": {"insert": 20, "update": 10},
    "gate_orders": {"insert": 100, "update": 30, "delete": 5},
}
#: Text the scripts write, spelled as escapes (Russian "new customer", "shipped"; Ukrainian "Lviv").
NEW_CUSTOMER = "\u043d\u043e\u0432\u044b\u0439 \u043a\u043b\u0438\u0435\u043d\u0442"
SHIPPED = "\u043e\u0442\u043f\u0440\u0430\u0432\u043b\u0435\u043d"
LVIV = "\u041b\u044c\u0432\u0456\u0432"


@dataclass(frozen=True)
class Cycle:
    """One run of the scenario's DAGs; `changes`: the source is changed by the script just before it."""

    name: str
    changes: bool


CYCLES = (Cycle("initial", False), Cycle("delta", True), Cycle("idle", False))
#: The multi-host leg's fourth cycle: the script runs again, the paused extract is killed and retried.
RETRY = Cycle("retry", True)

SCRIPTS = {
    "postgres": """
INSERT INTO gate_customers (name, tier) SELECT 'new customer ' || g, 'free' FROM generate_series(1, <<c_insert>>) g;
INSERT INTO gate_orders (customer_id, amount, status) SELECT 1 + g % 1000, 9.99 * g, 'new' FROM generate_series(1, <<o_insert>>) g;
UPDATE gate_orders SET status = 'shipped', updated_at = clock_timestamp() WHERE id IN (SELECT id FROM gate_orders ORDER BY id LIMIT <<o_update>>);
UPDATE gate_customers SET tier = 'team', updated_at = clock_timestamp() WHERE id IN (SELECT id FROM gate_customers ORDER BY id LIMIT <<c_update>>);
DELETE FROM gate_orders WHERE id IN (SELECT id FROM gate_orders WHERE id <= <<seed_orders>> ORDER BY id DESC LIMIT <<o_delete>>);
""",
    # Every update changes a value (MySQL logs no event and keeps `updated_at` for a row an UPDATE leaves as it was).
    "mysql": """
SET SESSION cte_max_recursion_depth = 10000;
INSERT INTO gate_customers (name, tier) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < <<c_insert>>)
  SELECT CONCAT('<<new_customer>> ', n), 'free' FROM g;
INSERT INTO gate_orders (customer_id, amount, status) WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < <<o_insert>>)
  SELECT 1 + n % 1000, 9.99 * n, 'new' FROM g;
UPDATE gate_orders SET status = 'shipped' ORDER BY id LIMIT <<o_update>>;
UPDATE gate_customers SET tier = 'vip' ORDER BY id LIMIT <<c_update>>;
DELETE FROM gate_orders WHERE id <= <<seed_orders>> ORDER BY id DESC LIMIT <<o_delete>>;
""",
    # The last statement waits until the Agent's capture job has read the DELETE, so a CDC run right after sees every
    # change (`cdc.lsn_time_mapping` is filled by the capture job; its times are DATETIME, 3 ms ticks).
    "mssql": """
USE rivet;
SET NOCOUNT ON;
WITH g AS (SELECT TOP (<<c_insert>>) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects)
INSERT INTO dbo.gate_customers (name, tier) SELECT CONCAT(N'<<new_customer>> ', n), 'free' FROM g;
WITH g AS (SELECT TOP (<<o_insert>>) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_objects)
INSERT INTO dbo.gate_orders (customer_id, amount, status, placed_at, payload)
  SELECT 1 + n % 1000, 9.99 * n, N'new', TODATETIMEOFFSET(SYSUTCDATETIME(), '-05:30'), 0x00FF10 FROM g;
UPDATE dbo.gate_orders SET status = N'<<shipped>>', updated_at = SYSUTCDATETIME()
  WHERE id IN (SELECT TOP (<<o_update>>) id FROM dbo.gate_orders ORDER BY id);
UPDATE dbo.gate_customers SET tier = 'vip', updated_at = SYSUTCDATETIME()
  WHERE id IN (SELECT TOP (<<c_update>>) id FROM dbo.gate_customers ORDER BY id);
WAITFOR DELAY '00:00:00.200';
DECLARE @before DATETIME = DATEADD(MILLISECOND, -20, SYSDATETIME());
DELETE FROM dbo.gate_orders WHERE id IN (SELECT TOP (<<o_delete>>) id FROM dbo.gate_orders WHERE id <= <<seed_orders>> ORDER BY id DESC);
DECLARE @waited INT = 0;
WHILE NOT EXISTS (SELECT 1 FROM cdc.lsn_time_mapping WHERE tran_begin_time >= @before) AND @waited < 180
BEGIN WAITFOR DELAY '00:00:01'; SET @waited += 1; END;
IF @waited >= 180 THROW 50000, 'the CDC capture job did not read the changes within 180 s', 1;
""",
    "mongodb": """
const now = new Date();
const some = db.gate_customers.find().sort({_id: 1}).limit(<<c_update>>).toArray().map(d => d._id);
db.gate_customers.insertMany(Array.from({length: <<c_insert>>}, (_, i) => ({
  name: "<<new_customer>> " + (i + 1), tier: "free", addr: {city: "<<lviv>>", tags: []}, balance: NumberDecimal("0.00"),
  updated_at: now})));
db.gate_orders.insertMany(Array.from({length: <<o_insert>>}, (_, i) => ({
  _id: NumberLong(String(<<first_new>> + i)), customer_id: some[i % <<c_update>>], amount: NumberDecimal((9.99 * (i + 1)).toFixed(2)),
  items: [], status: "new", placed_at: now})));
db.gate_orders.updateMany({_id: {$lte: NumberLong("<<o_update>>")}}, {$set: {status: "<<shipped>>", updated_at: now}});
db.gate_customers.updateMany({_id: {$in: some}}, {$set: {tier: "vip", "addr.tags": ["vip"]}});
db.gate_orders.deleteMany({_id: {$gte: NumberLong("<<first_gone>>"), $lte: NumberLong("<<seed_orders>>")}});
""",
    "oracle": """
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = rivet;
INSERT INTO gate_customers (name, tier) SELECT N'<<new_customer>> ' || LEVEL, 'free' FROM dual CONNECT BY LEVEL <= <<c_insert>>;
INSERT INTO gate_orders (customer_id, amount, status, placed_at, payload, doc)
  SELECT 1 + MOD(LEVEL, 1000), 9.99 * LEVEL, N'new', SYSTIMESTAMP AT TIME ZONE '-05:30', HEXTORAW('00FF10'),
    TO_BLOB(HEXTORAW('00FF10'))
  FROM dual CONNECT BY LEVEL <= <<o_insert>>;
UPDATE gate_orders SET status = N'<<shipped>>', updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE id <= <<o_update>>;
UPDATE gate_customers SET tier = 'vip', updated_at = SYS_EXTRACT_UTC(SYSTIMESTAMP) WHERE id <= <<c_update>>;
DELETE FROM gate_orders WHERE id BETWEEN <<first_gone>> AND <<seed_orders>>;
COMMIT;
EXIT
""",
}


def script(engine: str) -> str:
    """The delta script of `engine` ("postgres" is the scenario's own source), its counts read from `DELTA`."""
    c, o = DELTA["gate_customers"], DELTA["gate_orders"]
    values = {
        "c_insert": c["insert"],
        "c_update": c["update"],
        "o_insert": o["insert"],
        "o_update": o["update"],
        "o_delete": o["delete"],
        "seed_orders": SEED_ORDERS,
        "first_new": SEED_ORDERS + 1,
        "first_gone": SEED_ORDERS - o["delete"] + 1,
        "new_customer": NEW_CUSTOMER,
        "shipped": SHIPPED,
        "lviv": LVIV,
    }
    return re.sub(r"<<(\w+)>>", lambda m: str(values[m.group(1)]), SCRIPTS[engine])


def named(name: str, cycles: Sequence[Cycle] = CYCLES) -> Cycle:
    """The cycle called `name`."""
    return next(c for c in cycles if c.name == name)


def ops(name: str, cycles: Sequence[Cycle] = CYCLES) -> dict[str, dict[str, int]]:
    """The operations per table the script made in the cycle `name` (none in a cycle that changes nothing)."""
    return {t: dict(n) if named(name, cycles).changes else {} for t, n in DELTA.items()}


def kept_by_batch(name: str, table: str, cycles: Sequence[Cycle] = CYCLES) -> int:
    """Keys of `table` an incremental extract still holds after the cycle `name` although the source deleted them: each
    script run so far deleted `DELTA`'s count. Oracle's table names are upper-case."""
    runs = sum(c.changes for c in cycles[: cycles.index(named(name, cycles)) + 1])
    return runs * DELTA.get(table.lower(), {}).get("delete", 0)
