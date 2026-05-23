
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

try:
    # inotify is Linux-only. We feature-detect; on macOS/CI we poll.
    from inotify_simple import INotify, flags as inotify_flags  # type: ignore
    _HAS_INOTIFY = True
except ImportError:
    _HAS_INOTIFY = False


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

class LogWriter:
    """
    Append-only NDJSON writer. One instance per (run, job step).

    Thread-safety: the runner only writes from one place (the log-streaming
    loop in JobRunner._stream_logs), so we don't add a lock. If you ever
    write from multiple threads, wrap .write() in a threading.Lock.
    """

    def __init__(self, path: str, job: str):
        self.path = path
        self.job = job
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        # Line-buffered text mode would be tempting, but we want the raw
        # bytes flushed on every line so SSE readers see them instantly.
        # Open in binary append, write JSON+\n, flush.
        self._fh = open(path, "ab", buffering=0)
        self._closed = False

    def write(self, line: str) -> None:
        if self._closed:
            return
        # Strip any embedded \r so JSON stays on one line. We DO NOT strip \n
        # inside the line value; json.dumps will escape it.
        cleaned = line.rstrip("\r\n")
        record = {
            "ts": _now_iso(),
            "job": self.job,
            "line": cleaned,
        }
        payload = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
        self._fh.write(payload)
        # buffering=0 means write() goes straight to the OS. We don't
        # fsync per line (would crush IOPS); we rely on the page cache,
        # which readers on the same host see immediately.

    def close(self) -> None:
        if self._closed:
            return
        try:
            eof = {"ts": _now_iso(), "job": self.job, "eof": True}
            self._fh.write((json.dumps(eof) + "\n").encode("utf-8"))
            self._fh.flush()
            os.fsync(self._fh.fileno())
        finally:
            self._fh.close()
            self._closed = True


def _now_iso() -> str:
    # millisecond-precision ISO 8601, always UTC, always with the Z suffix.
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------------------
# Reader / tailer
# ---------------------------------------------------------------------------

async def tail_log(
    path: str,
    follow: bool = True,
    from_offset: int = 0,
    idle_timeout_s: float = 60.0,
) -> AsyncIterator[bytes]:
    """
    Async generator yielding SSE-framed bytes from the log file.

    Phase 1 (backlog): read everything from `from_offset` to current EOF
    one line at a time, yielding SSE frames. Never reads the whole file
    into memory.

    Phase 2 (follow): if follow=True and we haven't seen the EOF sentinel,
    wait for new bytes (inotify if available, otherwise poll) and stream
    them as they arrive.

    Closes when:
      - follow=False and we hit current EOF, OR
      - we read a record with "eof": true, OR
      - idle for idle_timeout_s seconds with no new bytes.
    """
    # Wait briefly for the file to exist. The scheduler may create the
    # run row before the runner has opened the log file.
    deadline = time.monotonic() + 5.0
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            yield _sse_event({"error": "log not found", "path": path})
            return
        await asyncio.sleep(0.05)

    # We open the file in binary read mode and seek. The OS page cache
    # makes this cheap; we never load more than one line at a time.
    with open(path, "rb") as fh:
        fh.seek(from_offset)
        leftover = b""
        saw_eof = False

        # --- Phase 1: drain backlog ---
        for line in _read_lines(fh, leftover_holder=lambda: leftover):
            if line is None:
                break
            saw_eof = saw_eof or _is_eof_line(line)
            yield _sse_event_raw(line)

        if not follow or saw_eof:
            return

        # --- Phase 2: tail ---
        if _HAS_INOTIFY:
            async for chunk in _tail_inotify(fh, path, idle_timeout_s):
                if _is_eof_line(chunk):
                    yield _sse_event_raw(chunk)
                    return
                yield _sse_event_raw(chunk)
        else:
            async for chunk in _tail_poll(fh, idle_timeout_s):
                if _is_eof_line(chunk):
                    yield _sse_event_raw(chunk)
                    return
                yield _sse_event_raw(chunk)


def _read_lines(fh, leftover_holder=None):
    """
    Generator that yields complete lines from `fh`, starting at the
    current position, until EOF. Holds at most one partial line in memory.

    Yields None when it runs out of complete lines (caller decides whether
    to wait for more).
    """
    buf = b""
    while True:
        chunk = fh.read(8192)
        if not chunk:
            # If we have a leftover partial line, leave it for the next call;
            # the position is already past EOF of the previous read.
            if buf:
                # Walk back so a future read sees the partial line again.
                fh.seek(-len(buf), os.SEEK_CUR)
            yield None
            return
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            yield line


async def _tail_inotify(fh, path: str, idle_timeout_s: float) -> AsyncIterator[bytes]:
    """Tail using inotify IN_MODIFY. Returns one complete line per yield."""
    inotify = INotify()
    inotify.add_watch(path, inotify_flags.MODIFY | inotify_flags.CLOSE_WRITE)
    loop = asyncio.get_event_loop()
    last_data = time.monotonic()
    buf = b""

    try:
        while True:
            # First, drain anything already on disk.
            while True:
                chunk = fh.read(8192)
                if not chunk:
                    break
                last_data = time.monotonic()
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    yield line

            if time.monotonic() - last_data > idle_timeout_s:
                return

            # Wait for inotify in a thread so we don't block the event loop.
            events = await loop.run_in_executor(
                None, lambda: inotify.read(timeout=1000)
            )
            if not events:
                # 1s tick with no events -- check idle and loop.
                continue
    finally:
        inotify.close()


async def _tail_poll(fh, idle_timeout_s: float) -> AsyncIterator[bytes]:
    """Fallback tailer for systems without inotify (macOS, tests)."""
    last_data = time.monotonic()
    buf = b""
    while True:
        chunk = fh.read(8192)
        if chunk:
            last_data = time.monotonic()
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                yield line
        else:
            if time.monotonic() - last_data > idle_timeout_s:
                return
            await asyncio.sleep(0.1)


def _sse_event(payload: dict) -> bytes:
    """Frame a dict as an SSE 'data:' event."""
    return _sse_event_raw(json.dumps(payload).encode("utf-8"))


def _sse_event_raw(line: bytes) -> bytes:
    """
    Frame an already-JSON line as an SSE event. We don't re-parse the JSON
    we just read; we just wrap it. This keeps the hot path allocation-free
    apart from the prefix bytes.
    """
    return b"data: " + line + b"\n\n"


def _is_eof_line(line: bytes) -> bool:
    # Cheap pre-check before JSON parse.
    if b'"eof"' not in line:
        return False
    try:
        return bool(json.loads(line).get("eof"))
    except (ValueError, json.JSONDecodeError):
        return False


# ---------------------------------------------------------------------------
# FastAPI integration
# ---------------------------------------------------------------------------
# The platform's HTTP layer wires this up roughly as:
#
#   from fastapi import APIRouter
#   from fastapi.responses import StreamingResponse
#   from engine.logs import tail_log
#
#   router = APIRouter()
#
#   @router.get("/runs/{run_id}/logs")
#   async def stream_logs(run_id: str, follow: bool = True, offset: int = 0):
#       path = f"/var/forge/logs/{run_id}.log"
#       return StreamingResponse(
#           tail_log(path, follow=follow, from_offset=offset),
#           media_type="text/event-stream",
#           headers={
#               "Cache-Control": "no-cache",
#               "X-Accel-Buffering": "no",   # disable nginx response buffering
#               "Connection": "keep-alive",
#           },
#       )
#
# That's all that's needed. tail_log is an async generator, so FastAPI
# streams it back to the client a chunk at a time. The Python process
# holds one open file descriptor and at most one partial line in memory
# per connected client, regardless of how big the log gets.
