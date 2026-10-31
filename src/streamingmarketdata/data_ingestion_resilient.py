"""Collect Kraken book JSON with reconnects and separate operational logging.

Run with ``python -m streamingmarketdata.data_ingestion_resilient``.
Each connection requests new snapshots; updates missed while disconnected
cannot be recovered by this collector.
"""

import asyncio
import json
import logging
import random
import signal
import ssl
import sys
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from decimal import Decimal
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from websockets.asyncio.client import ClientConnection, connect
from websockets.client import process_exception
from websockets.exceptions import ConnectionClosed, InvalidStatus

URL = "wss://ws.kraken.com/v2"
SYMBOLS = ["BTC/USD", "ETH/USD"]
DEPTH = 25
LOG_DIRECTORY = Path("logs")
STARTUP_TIMEOUT = 30.0
IDLE_TIMEOUT = 45.0
MIN_RETRY_DELAY = 5.0
MAX_RETRY_DELAY = 60.0
STABLE_CONNECTION_TIME = 60.0


class ReconnectRequired(RuntimeError):
    """The current session cannot safely continue."""


class SubscriptionRejected(RuntimeError):
    """The subscription must be corrected before trying again."""


class DataWriteError(RuntimeError):
    """Stop collection when received data cannot be saved."""


class StrictFileHandler(TimedRotatingFileHandler):
    def handleError(self, record: logging.LogRecord) -> None:
        # Standard logging otherwise prints an error and discards the record.
        raise DataWriteError("Failed to save a book message") from sys.exception()


class BookDataWriter:
    def __init__(self, handler: StrictFileHandler):
        self.handler = handler

    async def write(self, raw_message: str) -> None:
        # Remove JSON whitespace newlines, preserving the exact numeric text.
        line = raw_message.replace("\r", "").replace("\n", "")
        record = logging.LogRecord("kraken.book.data", logging.INFO, "", 0, line, (), None)
        work = asyncio.create_task(asyncio.to_thread(self.handler.handle, record))
        try:
            # Writes are awaited sequentially to preserve receive order.
            await asyncio.shield(work)
        except asyncio.CancelledError:
            # Finish an in-flight write before the handler is closed.
            await work
            raise


@contextmanager
def open_logs(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    events = logging.getLogger("kraken.book.resilient")
    events.setLevel(logging.INFO)
    events.propagate = False
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    formatter.converter = time.gmtime

    with ExitStack() as stack:
        console = logging.StreamHandler()
        stack.callback(console.close)
        console.setFormatter(formatter)
        events.addHandler(console)
        stack.callback(events.removeHandler, console)

        diagnostics = TimedRotatingFileHandler(
            directory / "kraken_book_L2_resilient.events.log",
            when="midnight", backupCount=7, encoding="utf-8", utc=True,
        )
        stack.callback(diagnostics.close)
        diagnostics.setFormatter(formatter)
        events.addHandler(diagnostics)
        stack.callback(events.removeHandler, diagnostics)

        data = StrictFileHandler(
            directory / "kraken_book_L2_resilient.jsonl",
            when="m", interval=15, backupCount=0, encoding="utf-8", utc=True,
        )
        stack.callback(data.close)
        data.setFormatter(logging.Formatter("%(message)s"))
        yield BookDataWriter(data), events


@dataclass
class SessionState:
    ready_at: float | None = None


def decode_message(raw_message: str | bytes) -> tuple[str, dict]:
    try:
        raw = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
        message = json.loads(raw, parse_float=Decimal)
    except (UnicodeError, ValueError) as error:
        # Skipping a malformed message could silently lose a book update.
        raise ReconnectRequired("Invalid JSON received; requesting fresh snapshots") from error
    if not isinstance(message, dict):
        raise ReconnectRequired("Expected a JSON object")
    return raw, message


async def collect_session(
    ws: ClientConnection, writer: BookDataWriter, events: logging.Logger, state: SessionState,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + STARTUP_TIMEOUT
    pending_acks = set(SYMBOLS)
    pending_snapshots = set(SYMBOLS)
    subscription = {
        "method": "subscribe",
        "params": {"channel": "book", "symbol": SYMBOLS, "depth": DEPTH, "snapshot": True},
        "req_id": 1,
    }
    async with asyncio.timeout(STARTUP_TIMEOUT):
        await ws.send(json.dumps(subscription))

    while True:
        timeout = IDLE_TIMEOUT
        if pending_acks or pending_snapshots:
            timeout = min(timeout, deadline - loop.time())
            if timeout <= 0:
                raise ReconnectRequired(
                    f"Subscription startup timed out: acknowledgements={sorted(pending_acks)}, "
                    f"snapshots={sorted(pending_snapshots)}"
                )
        try:
            raw_message = await asyncio.wait_for(ws.recv(), timeout=timeout)
        except TimeoutError as error:
            raise ReconnectRequired(
                f"No message before timeout; acknowledgements={sorted(pending_acks)}, "
                f"snapshots={sorted(pending_snapshots)}"
            ) from error

        raw, message = decode_message(raw_message)
        if message.get("method") == "subscribe":
            if message.get("success") is not True:
                reason = str(message.get("error", "Unknown subscription error"))
                if reason.startswith("EService:") or "rate limit" in reason.lower():
                    raise ReconnectRequired(f"Subscription temporarily unavailable: {reason}")
                raise SubscriptionRejected(reason)
            result = message.get("result")
            if (
                not isinstance(result, dict)
                or result.get("channel") != "book"
                or result.get("symbol") not in SYMBOLS
                or result.get("depth") != DEPTH
                or message.get("req_id") != 1
            ):
                raise ReconnectRequired("Unexpected subscription acknowledgement")
            pending_acks.discard(result["symbol"])
            events.info("Subscribed: %s, depth=%s", result["symbol"], DEPTH)
            for warning in result.get("warnings", []):
                events.warning("Subscription warning: %s", warning)
        elif message.get("channel") == "book":
            kind, data = message.get("type"), message.get("data")
            if kind not in {"snapshot", "update"} or not isinstance(data, list) or not data:
                raise ReconnectRequired("Malformed book message")
            symbols = set()
            for item in data:
                if not isinstance(item, dict) or item.get("symbol") not in SYMBOLS:
                    raise ReconnectRequired("Unexpected book symbol")
                symbol = item["symbol"]
                if kind == "update" and symbol in pending_snapshots:
                    raise ReconnectRequired(f"Received {symbol} update before its snapshot")
                symbols.add(symbol)
            await writer.write(raw)
            if kind == "snapshot":
                pending_snapshots.difference_update(symbols)
                events.info("Saved fresh snapshot: %s", ", ".join(sorted(symbols)))
        elif message.get("channel") == "status":
            events.info("Kraken status: %s", message.get("data"))
        elif message.get("channel") == "heartbeat":
            pass
        elif "error" in message:
            raise ReconnectRequired(f"Kraken error: {message['error']}")
        else:
            events.debug("Unhandled message: %s", message)

        if not pending_acks and not pending_snapshots and state.ready_at is None:
            state.ready_at = loop.time()
            events.info("All subscriptions acknowledged and snapshots saved")


def is_retryable(error: Exception) -> bool:
    if isinstance(error, ssl.SSLCertVerificationError):
        return False
    if isinstance(error, (ConnectionClosed, ReconnectRequired)):
        return True
    if isinstance(error, InvalidStatus):
        return error.response.status_code in {408, 429, 500, 502, 503, 504}
    return process_exception(error) is None


async def ingest(writer: BookDataWriter, events: logging.Logger) -> None:
    delay = MIN_RETRY_DELAY
    loop = asyncio.get_running_loop()
    while True:
        state = SessionState()
        try:
            events.info("Connecting to %s", URL)
            async with connect(
                URL, open_timeout=20, ping_interval=20, ping_timeout=30,
                close_timeout=10, max_queue=128, max_size=4 * 1024 * 1024,
                logger=events.getChild("websocket"),
            ) as ws:
                await collect_session(ws, writer, events, state)
        except Exception as error:
            if not is_retryable(error):
                raise
            if state.ready_at is not None and loop.time() - state.ready_at >= STABLE_CONNECTION_TIME:
                delay = MIN_RETRY_DELAY
            wait = random.uniform(delay, min(MAX_RETRY_DELAY, delay * 1.25))
            events.warning(
                "Connection interrupted (%s: %s). Data gap; reconnecting in %.1fs "
                "and requesting fresh snapshots.", type(error).__name__, error, wait,
                exc_info=True,
            )
            await asyncio.sleep(wait)
            delay = min(MAX_RETRY_DELAY, delay * 2)


async def main() -> None:
    with open_logs(LOG_DIRECTORY) as (writer, events):
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        sigterm_installed = False
        try:
            loop.add_signal_handler(signal.SIGTERM, task.cancel)
            sigterm_installed = True
        except (NotImplementedError, RuntimeError):
            pass  # asyncio.run still handles Ctrl+C on platforms without signal handlers.
        try:
            await ingest(writer, events)
        except asyncio.CancelledError:
            events.info("Stopped; closing the connection and flushing logs")
            raise
        except Exception:
            events.exception("Ingestion stopped after an unrecoverable error")
            raise
        finally:
            if sigterm_installed:
                loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception as error:
        print(f"Fatal ingestion error: {error}", file=sys.stderr)
        raise SystemExit(1)
