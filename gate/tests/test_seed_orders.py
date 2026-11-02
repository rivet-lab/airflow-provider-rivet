"""Every engine seeds the scenario's orders with SEED_ORDERS rows, the count the cycle's arithmetic and the migration rely on."""

from __future__ import annotations

import re

import pytest

from gate import cycle_effect, migrate, scenario, sources

BOUND = {
    "postgres": (scenario.SEED, r"INSERT INTO gate_orders[^;]*generate_series\(1, (\d+)\)"),
    "mysql": (sources.MYSQL.seed, r"INSERT INTO gate_orders[^;]*WHERE n < (\d+)\)"),
    "mssql": (sources.MSSQL.seed, r"TOP \((\d+)\)[^;]*INTO dbo\.gate_orders"),
    "mongodb": (sources.MONGODB.seed, r"n <= (\d+); n\+\+\) orders\.push"),
    "oracle": (sources.ORACLE.seed, r"INSERT INTO gate_orders[^;]*CONNECT BY LEVEL <= (\d+);"),
}


@pytest.mark.parametrize("engine", sorted(BOUND))
def test_each_seed_inserts_seed_orders_orders(engine: str) -> None:
    text, pattern = BOUND[engine]
    assert re.findall(pattern, text) == [str(cycle_effect.SEED_ORDERS)], engine


def test_the_migration_changes_only_rows_the_seed_inserted() -> None:
    ids = [int(i) for sql, _ in migrate.STEPS.values() for i in re.findall(r"WHERE id = (\d+)", sql)]
    assert ids and max(ids) <= cycle_effect.SEED_ORDERS
