"""Archive Kraken book messages while recovering from transient connection errors.

Run with ``python -m streamingmarketdata.data_ingestion_resilient``. Configure
symbols, book depth, log directory, and timeouts through the module constants;
all timeout and retry-delay constants are expressed in seconds.

Book messages retain their original numeric text in JSONL files, which rotate
every 15 minutes without deleting old files. Operational messages go to stderr
and a separate UTC diagnostic log, which rotates daily with seven backups.

Each connection requests fresh snapshots before accepting updates for a symbol.
The collector validates message envelopes, but does not reconstruct books or
verify checksums. Updates missed while disconnected cannot be recovered here.
"""

import asyncio
import json
import logging
import random
import signal
import ssl
import sys
import time
from collections.abc import Iterator
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
    """Signal that an unsafe or incomplete session needs fresh snapshots."""


class SubscriptionRejected(RuntimeError):
    """Signal a subscription rejection that is not classified as temporary."""


class DataWriteError(RuntimeError):
    """Signal a book-log failure that must stop collection rather than reconnect."""


class StrictFileHandler(TimedRotatingFileHandler):
    """Rotate book logs while propagating write failures to the collector."""

    def handleError(self, record: logging.LogRecord) -> None:
        """Turn a logging failure into a fatal collector error.

        Standard logging may report a failed write and discard the record.
        Raising here prevents collection from continuing after that data loss.

        Args:
            record (logging.LogRecord): Record whose emission failed.

        Returns:
            None: This method always raises instead of returning normally.

        Raises:
            DataWriteError: Always, chained from the active logging exception.
        """
        raise DataWriteError("Failed to save a book message") from sys.exception()


class BookDataWriter:
    """Write raw JSON through a rotating handler without blocking the event loop.

    Callers must await writes sequentially to preserve receive order. The owner
    of the supplied handler remains responsible for closing it.

    Attributes:
        handler (StrictFileHandler): Destination for book-message records.
    """

    def __init__(self, handler: StrictFileHandler) -> None:
        """Store the file handler used for subsequent writes.

        Args:
            handler (StrictFileHandler): Configured handler owned by the caller.

        Returns:
            None.
        """
        self.handler = handler

    async def write(self, raw_message: str) -> None:
        """Save one JSON message in a worker thread and wait for completion.

        Physical carriage returns and newlines are removed to keep each record
        on one line. Numeric text and escaped newlines inside strings remain
        intact. This method expects JSON that has already been decoded safely.
        Cancellation waits for the protected write before propagating, allowing
        the handler to be closed without interrupting that write.

        Args:
            raw_message (str): Original, valid JSON text received from Kraken.

        Returns:
            None: Completion indicates the handler finished writing the record;
                it does not guarantee an fsync to persistent storage.

        Raises:
            DataWriteError: The handler could not save the record.
            asyncio.CancelledError: Collection was cancelled during the write.
        """
        line = raw_message.replace("\r", "").replace("\n", "")
        record = logging.LogRecord("kraken.book.data", logging.INFO, "", 0, line, (), None)
        work = asyncio.create_task(asyncio.to_thread(self.handler.handle, record))
        try:
            await asyncio.shield(work)
        except asyncio.CancelledError:
            await work
            raise


@contextmanager
def open_logs(directory: Path) -> Iterator[tuple[BookDataWriter, logging.Logger]]:
    """Provide the data writer and event logger, then clean up their handlers.

    Diagnostics go to stderr and a daily UTC log with seven backups. Raw book
    messages go to a JSONL log that rotates every 15 minutes without automatic
    deletion. Handlers are removed and closed when the context exits, including
    when setup or collection raises an exception.

    Args:
        directory (Path): Log directory, created with missing parent directories.
            Relative paths are resolved against the process working directory.

    Yields:
        tuple[BookDataWriter, logging.Logger]: Writer for raw book messages and
            logger for operational events, valid within the context.

    Raises:
        OSError: The directory or a log file cannot be created or opened.
    """
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
    """Track when the current connection became ready for normal collection.

    Attributes:
        ready_at (float | None): Event-loop monotonic time when every requested
            symbol was acknowledged and its snapshot saved. None means startup
            has not completed. Used to decide when retry backoff can reset.
    """

    ready_at: float | None = None


def decode_message(raw_message: str | bytes) -> tuple[str, dict]:
    """Decode a JSON object while preserving raw text and decimal precision.

    Invalid input requests reconnection because skipping a message could omit
    a book update. Decimal JSON numbers become Decimal values; integer JSON
    numbers retain their normal int representation.

    Args:
        raw_message (str | bytes): WebSocket text or UTF-8 encoded message bytes.

    Returns:
        tuple[str, dict]: Original JSON text, decoded from UTF-8 if needed, and
            its dictionary representation for message routing.

    Raises:
        ReconnectRequired: Input is invalid UTF-8, invalid JSON, or does not
            contain a JSON object at its top level.
    """
    try:
        raw = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
        message = json.loads(raw, parse_float=Decimal)
    except (UnicodeError, ValueError) as error:
        raise ReconnectRequired("Invalid JSON received; requesting fresh snapshots") from error
    if not isinstance(message, dict):
        raise ReconnectRequired("Expected a JSON object")
    return raw, message


async def collect_session(
    ws: ClientConnection, writer: BookDataWriter, events: logging.Logger, state: SessionState,
) -> None:
    """Subscribe and archive book messages for one established connection.

    Request fresh snapshots and track acknowledgements and snapshots separately
    for each configured symbol. A fixed startup deadline applies even while
    heartbeats arrive. After startup, each receive has an idle timeout.

    Save raw snapshots and subsequent updates sequentially. Status messages and
    subscription warnings go to the event logger; heartbeats are not archived.
    A symbol's updates are accepted after its snapshot has been saved, even if
    other symbols are still starting up. This function does not verify CRC32.

    Args:
        ws (ClientConnection): Open WebSocket connection owned by the caller.
        writer (BookDataWriter): Destination for original book-message JSON.
        events (logging.Logger): Destination for connection and service events.
        state (SessionState): Mutable state whose ready_at is set once all
            acknowledgements and snapshots have been received and saved.

    Returns:
        None: Runs until connection closure, failure, or cancellation; there is
            no normal return from the receive loop.

    Raises:
        ReconnectRequired: Messages are malformed, startup or reception stalls,
            or Kraken reports a temporary service problem.
        SubscriptionRejected: Kraken rejects the subscription permanently
            according to the collector's classification.
        ConnectionClosed: The WebSocket closes, normally or abnormally.
        TimeoutError: Sending the subscription exceeds the startup timeout.
        DataWriteError: A book message cannot be saved.
        asyncio.CancelledError: The session is cancelled during shutdown.
    """
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
    """Classify whether the same connection settings should be tried again.

    Retry connection closures, explicit session-recovery requests, and selected
    HTTP timeout, rate-limit, and server errors. Certificate verification
    failures are fatal. Other errors use websockets' default classification.

    Args:
        error (Exception): Failure raised while connecting or collecting data.

    Returns:
        bool: True if reconnection is allowed; False if the error must propagate.
    """
    if isinstance(error, ssl.SSLCertVerificationError):
        return False
    if isinstance(error, (ConnectionClosed, ReconnectRequired)):
        return True
    if isinstance(error, InvalidStatus):
        return error.response.status_code in {408, 429, 500, 502, 503, 504}
    return process_exception(error) is None


async def ingest(writer: BookDataWriter, events: logging.Logger) -> None:
    """Collect continuously, reconnecting after errors classified as temporary.

    Each attempt creates a new connection and session state. Failed sessions
    close before retrying with exponential backoff and bounded random jitter.
    The base delay resets only after a fully ready session has remained stable
    for STABLE_CONNECTION_TIME. Every reconnect requests fresh snapshots and
    reports a data gap; missed historical updates are not recovered.

    Connection opening, pong reception, and closing have timeouts of 20, 30,
    and 10 seconds respectively. Keepalive pings run every 20 seconds. Incoming
    messages are limited to 4 MiB, with a receive-buffer high-water mark of
    128 frames.

    Args:
        writer (BookDataWriter): Shared destination for raw book messages.
        events (logging.Logger): Logger for attempts, failures, and retry delays.

    Returns:
        None: Runs until cancellation or an unrecoverable error, without a
            normal return from the reconnection loop.

    Raises:
        Exception: A connection or collection error is classified as fatal,
            including subscription rejection or inability to save book data.
        asyncio.CancelledError: Collection or a retry wait is cancelled.
    """
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
    """Open logs and run the collector with shutdown and fatal-error reporting.

    Register SIGTERM cancellation where supported; asyncio.run handles Ctrl+C
    when this coroutine is the program entry point. Cancellation unwinds the
    connection context and lets a protected data write finish. The installed
    SIGTERM handler is removed before the log handlers are closed.

    Args:
        None: Configuration is read from this module's constants.

    Returns:
        None: No result is produced; normal operation continues until stopped.

    Raises:
        asyncio.CancelledError: Shutdown was requested; propagated after logging.
        Exception: Log setup fails or ingestion encounters an unrecoverable error.
            Ingestion failures are logged with their traceback before propagation.
    """
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
