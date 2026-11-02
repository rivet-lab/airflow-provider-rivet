"""The event names the rivet task log carries: one member per `"event"` value."""

from enum import StrEnum


class Event(StrEnum):
    """A JSON event name of rivet's task log, as rivet writes it."""

    APPLY_ALREADY_APPLIED = "rivet.apply.already_applied"
    BUSY = "rivet.busy"
    CDC_MAX_EVENTS = "rivet.cdc.max_events"
    CRASHED = "rivet.crashed"
    ERROR = "rivet.error"
    LOCK_ACQUIRED = "rivet.lock.acquired"
    LOCK_WAIT = "rivet.lock.wait"
    PLAN_SEALED = "rivet.plan.sealed"
    REFUSED = "rivet.refused"
    RESULT = "rivet.result"
    START = "rivet.start"
    STDERR = "rivet.stderr"
    STEP = "rivet.step"
    STOPPED = "rivet.stopped"
    UNIT = "rivet.unit"
    WARNING = "rivet.warning"
