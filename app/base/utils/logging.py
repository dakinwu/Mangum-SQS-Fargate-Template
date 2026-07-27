"""Structured logging.

Two API styles:
    log_event(action, module=, data=)                 - structured business events
    log_info/log_warning/log_error/log_debug(message, module=, **fields) - simple messages

Env-aware sink: on ENV=dev/prod, emits single-line JSON (so a log aggregator
that parses JSON lines - CloudWatch Logs Insights, Loki, whatever - can query
individual fields); on ENV=local, prints colored human-readable text instead.
"""

import json
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Optional


class Color:
    INFO = "\033[94m"
    OKCYAN = "\033[96m"
    WARNING = "\033[93m"
    ERROR = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"


_LEVEL_COLOR = {
    "DEBUG": Color.OKCYAN,
    "INFO": Color.INFO,
    "WARNING": Color.WARNING,
    "ERROR": Color.ERROR,
}


def _is_aws_runtime() -> bool:
    return os.getenv("ENV", "local").lower() in ("dev", "prod")


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _caller_source() -> str:
    """``filename:lineno:func`` of the code that called the public log_* fn.

    Must be invoked directly from a public log_* function so the stack depth
    is constant: [-1]=this helper, [-2]=public fn, [-3]=caller.
    """
    stack = traceback.extract_stack()
    if len(stack) >= 3:
        frame = stack[-3]
        return f"{frame.filename}:{frame.lineno}:{frame.name}"
    return ""


def _build_payload(
    level: str,
    *,
    action: Optional[str] = None,
    message: Optional[str] = None,
    module: Optional[str] = None,
    data: Optional[dict[str, Any]] = None,
    extra: Optional[dict[str, Any]] = None,
    source: Optional[str] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"level": level, "timestamp": _now_utc()}
    if module:
        payload["module"] = module
    if action:
        payload["action"] = action
    if message:
        payload["message"] = message
    if data:
        payload["data"] = data
    if extra:
        payload["extra"] = extra
    if source:
        payload["source"] = source
    return payload


def _emit(payload: dict[str, Any]) -> None:
    if _is_aws_runtime():
        print(json.dumps(payload, ensure_ascii=False, default=str))
        return
    level = payload.get("level", "INFO")
    color = _LEVEL_COLOR.get(level, Color.INFO)
    head_parts = [f"[{level}]"]
    if payload.get("module"):
        head_parts.append(f"[{payload['module']}]")
    if payload.get("action"):
        head_parts.append(str(payload["action"]))
    if payload.get("message"):
        head_parts.append(str(payload["message"]))
    head = " ".join(head_parts)
    detail = {k: payload[k] for k in ("data", "extra", "source") if k in payload}
    line = head if not detail else f"{head} {json.dumps(detail, ensure_ascii=False, default=str)}"
    print(f"{color}{Color.BOLD}{line}{Color.ENDC}")


def log_event(
    action: str,
    *,
    module: Optional[str] = None,
    level: str = "INFO",
    data: Optional[dict[str, Any]] = None,
    **extra: Any,
) -> None:
    source = _caller_source() if level in ("WARNING", "ERROR") else None
    _emit(_build_payload(level, action=action, module=module, data=data, extra=extra or None, source=source))


def _log_msg(level: str, message: str, module: Optional[str], extra: dict[str, Any], source: Optional[str] = None) -> None:
    _emit(_build_payload(level, message=message, module=module, extra=extra or None, source=source))


def log_debug(message: str, *, module: Optional[str] = None, **extra: Any) -> None:
    _log_msg("DEBUG", message, module, extra)


def log_info(message: str, *, module: Optional[str] = None, **extra: Any) -> None:
    _log_msg("INFO", message, module, extra)


def log_warning(message: str, *, module: Optional[str] = None, **extra: Any) -> None:
    _log_msg("WARNING", message, module, extra, source=_caller_source())


def log_error(message: str, *, module: Optional[str] = None, **extra: Any) -> None:
    """Auto-enriches error_type/error from the active exception when called
    inside an `except` block."""
    exc = sys.exc_info()[1]
    if exc is not None:
        extra.setdefault("error_type", type(exc).__name__)
        extra.setdefault("error", str(exc))
    _log_msg("ERROR", message, module, extra, source=_caller_source())
