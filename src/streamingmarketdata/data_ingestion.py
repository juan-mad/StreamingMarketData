"""Archive every received Kraken message and recover from connection errors.

Run with ``python -m streamingmarketdata.data_ingestion``. Configure
symbols, book depth, log directory, and timeouts through the module constants;
all timeout and retry-delay constants are expressed in seconds.

Every received message is archived before decoding or validation. JSONL records
contain a connection/session UUID, the machine's UTC reception timestamp,
original payload, and payload encoding. Files rotate every 15 minutes without
deleting old files. Operational messages go to stderr and a separate UTC log
with seven daily backups.

Each connection requests fresh snapshots before accepting updates for a symbol.
Every price level and metadata field is validated, and reconstructed books are
checked against Kraken's top-ten CRC32. Raw logs also retain acknowledgements,
heartbeats, and rejected messages. Updates missed during disconnects cannot be
recovered. Reception timestamps mark delivery to the application, not packet
arrival at the network interface or the time Kraken generated the message.
"""

import asyncio
import base64
import json
import logging
import random
import signal
import ssl
import sys
import time
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import NoReturn
from uuid import UUID, uuid4

from streamingmarketdata.book import Book, ChecksumFailureError
from streamingmarketdata.book_messages import SnapshotData, UpdateData
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
    """Signal an archive failure that must stop collection rather than reconnect."""


class StrictFileHandler(TimedRotatingFileHandler):
    """Rotate raw message logs while propagating write failures to the collector."""

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
        raise DataWriteError("Failed to archive a received message") from sys.exception()


class RawMessageWriter:
    """Archive timestamped raw messages without blocking the event loop.

    Callers must await writes sequentially to preserve receive order. The owner
    of the supplied handler remains responsible for closing it.

    Attributes:
        handler (StrictFileHandler): Destination for raw archive records.
    """

    def __init__(self, handler: StrictFileHandler) -> None:
        """Store the file handler used for subsequent writes.

        Args:
            handler (StrictFileHandler): Configured handler owned by the caller.

        Returns:
            None.
        """
        self.handler = handler

    async def write(
        self, raw_message: str | bytes, received_at: datetime, session_id: UUID,
    ) -> None:
        """Archive an original payload with its reception time and connection ID.

        Text payloads are stored as JSON strings, preserving numeric formatting,
        whitespace, and malformed content without parsing it. Binary payloads
        are base64 encoded. JSON escaping keeps each record on one physical line.
        Cancellation waits for the protected write before propagating, allowing
        the handler to be closed without interrupting that write.

        Args:
            raw_message (str | bytes): Unmodified payload returned by ws.recv().
            received_at (datetime): Timezone-aware machine time captured as soon
                as ws.recv() returns; converted to UTC for archival.
            session_id (UUID): Local identifier shared by all messages received
                over this WebSocket connection, including all subscribed symbols.

        Returns:
            None: Completion indicates the handler finished writing the record;
                it does not guarantee an fsync to persistent storage.

        Raises:
            DataWriteError: The handler could not save the record.
            ValueError: The reception timestamp lacks a timezone.
            asyncio.CancelledError: Collection was cancelled during the write.
        """
        if received_at.utcoffset() is None:
            raise ValueError("Reception timestamp must include a timezone")
        if isinstance(raw_message, bytes):
            payload = base64.b64encode(raw_message).decode("ascii")
            encoding = "base64"
        else:
            payload = raw_message
            encoding = "utf-8"
        line = json.dumps({
            "session_id": str(session_id),
            "received_at": received_at.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "payload": payload,
            "payload_encoding": encoding,
        }, separators=(",", ":"))
        record = logging.LogRecord("kraken.websocket.raw", logging.INFO, "", 0, line, (), None)
        work = asyncio.create_task(asyncio.to_thread(self.handler.handle, record))
        try:
            await asyncio.shield(work)
        except asyncio.CancelledError:
            await work
            raise


@contextmanager
def open_logs(directory: Path) -> Iterator[tuple[RawMessageWriter, logging.Logger]]:
    """Provide the data writer and event logger, then clean up their handlers.

    Diagnostics go to stderr and a daily UTC log with seven backups. Timestamped
    raw messages go to a JSONL log that rotates every 15 minutes without automatic
    deletion. Handlers are removed and closed when the context exits, including
    when setup or collection raises an exception.

    Args:
        directory (Path): Log directory, created with missing parent directories.
            Relative paths are resolved against the process working directory.

    Yields:
        tuple[RawMessageWriter, logging.Logger]: Writer for received messages and
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
            directory / "kraken_ws_raw.jsonl",
            when="m", interval=15, backupCount=0, encoding="utf-8", utc=True,
        )
        stack.callback(data.close)
        data.setFormatter(logging.Formatter("%(message)s"))
        yield RawMessageWriter(data), events


@dataclass
class SessionState:
    """Identify one connection attempt and track when its subscriptions are ready.

    Attributes:
        ready_at (float | None): Event-loop monotonic time when every requested
            symbol was acknowledged and its snapshot saved. None means startup
            has not completed. Used to decide when retry backoff can reset.
        session_id (UUID): Locally generated identifier for this connection
            attempt. A new state generates a new ID, independent of process
            restarts, symbols, and archive file rotation.
    """

    ready_at: float | None = None
    session_id: UUID = field(default_factory=uuid4)


def reject_json_constant(value: str) -> NoReturn:
    """Reject nonstandard NaN and infinity tokens during JSON decoding.

    Args:
        value (str): Non-finite numeric token encountered by the JSON decoder.

    Returns:
        NoReturn: This function always raises.

    Raises:
        ValueError: The token is not a valid finite JSON number.
    """
    raise ValueError(f"Non-finite JSON number: {value}")


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
        message = json.loads(raw, parse_float=Decimal, parse_constant=reject_json_constant)
    except (UnicodeError, ValueError) as error:
        raise ReconnectRequired("Invalid JSON received; requesting fresh snapshots") from error
    if not isinstance(message, dict):
        raise ReconnectRequired("Expected a JSON object")
    return raw, message


async def collect_session(
    ws: ClientConnection, writer: RawMessageWriter, events: logging.Logger, state: SessionState,
) -> None:
    """Archive all received messages and validate books for one connection.

    Request fresh snapshots and track acknowledgements and snapshots separately
    for each configured symbol. A fixed startup deadline applies even while
    heartbeats arrive. After startup, each receive has an idle timeout.

    Capture a UTC reception timestamp immediately after each receive completes,
    then archive the original payload before decoding or filtering. This includes
    acknowledgements, status messages, heartbeats, and malformed messages. Status
    messages and subscription warnings also go to the event logger.
    A symbol's updates are accepted after its snapshot has been validated and
    saved, even if other symbols are still starting up. Each item is validated
    before converting its levels to dictionaries, then its CRC32 is checked.
    Raw messages remain archived even when their validation or checksum fails.

    Args:
        ws (ClientConnection): Open WebSocket connection owned by the caller.
        writer (RawMessageWriter): Destination for timestamped original payloads.
        events (logging.Logger): Destination for connection and service events.
        state (SessionState): Mutable state whose ready_at is set once all
            acknowledgements and snapshots have been received and saved. Its
            session_id identifies every archived message from this connection.

    Returns:
        None: Runs until connection closure, failure, or cancellation; there is
            no normal return from the receive loop.

    Raises:
        ReconnectRequired: Messages are malformed, validation or CRC32 fails,
            startup or reception stalls, or Kraken reports a temporary problem.
        SubscriptionRejected: Kraken rejects the subscription permanently
            according to the collector's classification.
        ConnectionClosed: The WebSocket closes, normally or abnormally.
        TimeoutError: Sending the subscription exceeds the startup timeout.
        DataWriteError: A received message cannot be archived.
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

    current_books: dict[str, Book] = {}
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
            received_at = datetime.now(UTC)
        except TimeoutError as error:
            raise ReconnectRequired(
                f"No message before timeout; acknowledgements={sorted(pending_acks)}, "
                f"snapshots={sorted(pending_snapshots)}"
            ) from error

        await writer.write(raw_message, received_at, state.session_id)
        _, message = decode_message(raw_message)
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
            if kind not in ("snapshot", "update") or not isinstance(data, list) or not data:
                raise ReconnectRequired("Malformed book message")
            symbols = set()
            for item in data:
                if not isinstance(item, dict) or item.get("symbol") not in SYMBOLS:
                    raise ReconnectRequired("Unexpected book symbol")
                symbol = item["symbol"]
                if kind == "update" and symbol in pending_snapshots:
                    raise ReconnectRequired(f"Received {symbol} update before its snapshot")
                symbols.add(symbol)
            if kind == "snapshot":
                for item in data:
                    symbol = item["symbol"]
                    if symbol in current_books:
                        events.info("Received fresh snapshot for %s, replacing current book", symbol)
                    try:
                        snapshot = SnapshotData.model_validate(item)
                        current_books[symbol] = snapshot.to_book(depth=DEPTH)
                    except ValueError as error:
                        raise ReconnectRequired(f"Invalid snapshot for {symbol}: {error}") from error
                    except ChecksumFailureError as error:
                        raise ReconnectRequired(f"Snapshot checksum failed for {symbol}") from error
                pending_snapshots.difference_update(symbols)
                events.info("Saved and validated fresh snapshot: %s", ", ".join(sorted(symbols)))

            if kind == "update":
                for item in data:
                    symbol = item["symbol"]
                    if symbol not in current_books:
                        raise ReconnectRequired("Received update but no book is being tracked")

                    try:
                        update = UpdateData.model_validate(item).to_update()
                        current_books[symbol].apply_update(update)
                    except ValueError as error:
                        raise ReconnectRequired(f"Invalid update for {symbol}: {error}") from error
                    except ChecksumFailureError as error:
                        raise ReconnectRequired(f"Update checksum failed for {symbol}") from error

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


async def ingest(writer: RawMessageWriter, events: logging.Logger) -> None:
    """Collect continuously, reconnecting after errors classified as temporary.

    Each attempt creates new session state with a fresh UUID that is included in
    every archived message. Failed sessions close before retrying with
    exponential backoff and bounded random jitter.
    The base delay resets only after a fully ready session has remained stable
    for STABLE_CONNECTION_TIME. Every reconnect requests fresh snapshots and
    reports a data gap; missed historical updates are not recovered.

    Connection opening, pong reception, and closing have timeouts of 20, 30,
    and 10 seconds respectively. Keepalive pings run every 20 seconds. Incoming
    messages are limited to 4 MiB, with a receive-buffer high-water mark of
    128 frames.

    Args:
        writer (RawMessageWriter): Shared destination for timestamped raw messages.
        events (logging.Logger): Logger for attempts, failures, and retry delays.

    Returns:
        None: Runs until cancellation or an unrecoverable error, without a
            normal return from the reconnection loop.

    Raises:
        Exception: A connection or collection error is classified as fatal,
            including subscription rejection or inability to archive messages.
        asyncio.CancelledError: Collection or a retry wait is cancelled.
    """
    delay = MIN_RETRY_DELAY
    loop = asyncio.get_running_loop()
    while True:
        state = SessionState()
        try:
            events.info("Connecting to %s (session_id=%s)", URL, state.session_id)
            async with connect(
                URL, open_timeout=20, ping_interval=20, ping_timeout=30,
                close_timeout=10, max_queue=128, max_size=4 * 1024 * 1024,
                logger=events.getChild("websocket"),
            ) as ws:
                events.info("WebSocket connected (session_id=%s)", state.session_id)
                await collect_session(ws, writer, events, state)
        except Exception as error:
            if not is_retryable(error):
                raise
            if state.ready_at is not None and loop.time() - state.ready_at >= STABLE_CONNECTION_TIME:
                delay = MIN_RETRY_DELAY
            wait = random.uniform(delay, min(MAX_RETRY_DELAY, delay * 1.25))
            events.warning(
                "Connection interrupted (session_id=%s, %s: %s). Data gap; reconnecting in %.1fs "
                "and requesting fresh snapshots.", state.session_id, type(error).__name__, error, wait,
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
