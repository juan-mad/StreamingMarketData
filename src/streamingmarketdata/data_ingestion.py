"""Archive every received Kraken message and recover from connection errors.

Launch through ``python -m streamingmarketdata --config PATH``. Settings are
loaded from YAML by the application and passed explicitly to this component.

Every received message is archived before decoding or validation. JSONL records
contain a connection/session UUID, the machine's UTC reception timestamp,
original payload, and payload encoding. Files rotate every 15 minutes without
deleting old files. Operational messages go to stderr and a separate UTC log.
Rotation intervals, file names, and diagnostic retention come from YAML.

Validated books are sampled independently at the configured interval into
kraken_validated_books.jsonl. Samples retain the session ID, machine sampling
time, exchange timestamp, and exact decimal values. Sampling stops before a
failed session reconnects; both raw and sampled write failures stop collection.

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
import math
import random
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
from streamingmarketdata.configuration import AppConfig, IngestionSettings, LoggingSettings
from websockets.asyncio.client import ClientConnection, connect
from websockets.client import process_exception
from websockets.exceptions import ConnectionClosed, InvalidStatus


class ReconnectRequired(RuntimeError):
    """Signal that an unsafe or incomplete session needs fresh snapshots."""


class SubscriptionRejected(RuntimeError):
    """Signal a subscription rejection that is not classified as temporary."""


class DataWriteError(RuntimeError):
    """Signal an archive failure that must stop collection rather than reconnect."""


class StrictFileHandler(TimedRotatingFileHandler):
    """Rotate data logs while propagating write failures to the collector."""

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
        raise DataWriteError(f"Failed to write data log: {self.baseFilename}") from sys.exception()


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


class ValidatedBookSampleWriter:
    """Periodically save detached copies of the current validated books.

    Writes are awaited sequentially. The handler's owner must close it only
    after the sampling task has stopped and any active write has finished.

    Attributes:
        handler (StrictFileHandler): Destination for sampled book records.
    """

    def __init__(self, handler: StrictFileHandler) -> None:
        """Store the file handler used for writes.

        Args:
            handler (StrictFileHandler): Configured handler owned by the caller.

        Returns:
            None.
        """
        self.handler = handler

    def _write_records(self, records: list[dict]) -> None:
        """Append a detached batch as individual JSONL records in a worker thread.

        Args:
            records (list[dict]): JSON-compatible copies of sampled book states.

        Returns:
            None: The handler has emitted every record, without an fsync guarantee.

        Raises:
            DataWriteError: Writing or rotating the destination file fails.
            ValueError: A record contains a non-finite JSON number.
            TypeError: A record contains a value that JSON cannot serialize.
        """
        for item in records:
            line = json.dumps(item, separators=(",", ":"), allow_nan=False)
            record = logging.LogRecord("kraken.book.sample", logging.INFO, "", 0, line, (), None)
            self.handler.handle(record)

    async def write(self, records: list[dict]) -> None:
        """Write one detached batch while allowing network tasks to continue.

        Args:
            records (list[dict]): JSON-compatible records owned by this write;
                callers must not mutate them until this method finishes.

        Returns:
            None: All records have been emitted, or the batch was empty.

        Raises:
            DataWriteError: The handler cannot save the batch.
            ValueError: A record contains a non-finite JSON number.
            TypeError: A record cannot be serialized as JSON.
            asyncio.CancelledError: Cancellation propagates after an active
                write has finished, so closing the handler cannot interrupt it.
        """
        if not records:
            return
        work = asyncio.create_task(asyncio.to_thread(self._write_records, records))
        try:
            await asyncio.shield(work)
        except asyncio.CancelledError:
            await work
            raise

    async def save_books_periodically(
        self, books: dict[str, Book], session_id: UUID, interval: float,
    ) -> None:
        """Sample validated books on monotonic deadlines, skipping missed intervals.

        Copies are made without yielding to the receiver, then written in a
        worker thread. Decimal values are serialized as strings. The book's timestamp is
        the exchange timestamp; sampled_at is the machine's UTC sampling time.
        Empty books are skipped until validated snapshots become available.

        Args:
            books (dict[str, Book]): Current books maintained on the same event
                loop by the receiver; never pass these live objects to a thread.
            session_id (UUID): Identifier of the current connection.
            interval (float): Positive, finite sampling interval in seconds.

        Returns:
            None: Runs until cancelled or a write fails.

        Raises:
            ValueError: The interval is non-positive or non-finite.
            DataWriteError: A sampled batch cannot be saved.
            asyncio.CancelledError: The session or sampling task is stopped.
        """
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval must be positive and finite")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + interval

        while True:
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            sampled_at = datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
            records = [
                {
                    "session_id": str(session_id),
                    "sampled_at": sampled_at,
                    **book.model_dump(mode="json"),
                }
                for book in books.values()
            ]

            if records:
                await self.write(records)

            deadline += interval
            if deadline <= loop.time():
                deadline = loop.time() + interval


@contextmanager
def open_logs(
    directory: Path, settings: LoggingSettings,
) -> Iterator[tuple[RawMessageWriter, ValidatedBookSampleWriter, logging.Logger]]:
    """Provide raw and sampled data writers and a logger, then close their handlers.

    Diagnostics go to stderr and a daily UTC log with configured retention. Timestamped
    raw messages and sampled validated books go to separate JSONL logs that
    rotate at the configured interval without automatic deletion. Handlers are removed
    and closed when the context exits, including when setup or collection raises
    an exception.

    Args:
        directory (Path): Log directory, created with missing parent directories.
            Relative paths are resolved against the process working directory.
        settings (LoggingSettings): File names, rotation, retention, and log level.

    Yields:
        tuple[RawMessageWriter, ValidatedBookSampleWriter, logging.Logger]: Raw
            writer, sampled-book writer, and event logger, valid within the context.

    Raises:
        OSError: The directory or a log file cannot be created or opened.
    """
    directory.mkdir(parents=True, exist_ok=True)
    events = logging.getLogger("kraken.book.resilient")
    events.setLevel(settings.level)
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
            directory / settings.events_filename,
            when="midnight", backupCount=settings.diagnostic_backups, encoding="utf-8", utc=True,
        )
        stack.callback(diagnostics.close)
        diagnostics.setFormatter(formatter)
        events.addHandler(diagnostics)
        stack.callback(events.removeHandler, diagnostics)

        data = StrictFileHandler(
            directory / settings.raw_filename,
            when="m", interval=settings.rotation_minutes, backupCount=0, encoding="utf-8", utc=True,
        )
        stack.callback(data.close)
        data.setFormatter(logging.Formatter("%(message)s"))

        samples = StrictFileHandler(
            directory / settings.samples_filename,
            when="m", interval=settings.rotation_minutes, backupCount=0, encoding="utf-8", utc=True,
        )
        stack.callback(samples.close)
        samples.setFormatter(logging.Formatter("%(message)s"))
        yield RawMessageWriter(data), ValidatedBookSampleWriter(samples), events


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
    sample_writer: ValidatedBookSampleWriter, settings: IngestionSettings,
) -> None:
    """Supervise reception and periodic book saving for one WebSocket session.

    A sampler failure cancels reception. When reception ends, the current books
    are cleared and the sampler is cancelled and awaited, including its active
    write, before this function exits. Storage failures take priority over a
    simultaneous reconnectable failure.

    Args:
        ws (ClientConnection): Open connection owned by the caller.
        writer (RawMessageWriter): Destination for every received raw message.
        events (logging.Logger): Logger for subscription and service events.
        state (SessionState): Session identifier and subscription readiness state.
        sample_writer (ValidatedBookSampleWriter): Writer for periodic book states;
            its handler remains open until this session has finished.
        settings (IngestionSettings): Subscription, timeout, and sampling settings.

    Returns:
        None: Runs until reception ends, an error occurs, or shutdown is requested.

    Raises:
        ReconnectRequired: Incoming data, its CRC32, or session readiness fails.
        ConnectionClosed: The WebSocket closes normally or abnormally.
        SubscriptionRejected: Kraken permanently rejects the subscription.
        DataWriteError: Raw or sampled data cannot be saved.
        ValueError: The sampling interval is invalid.
        asyncio.CancelledError: The session is cancelled during shutdown.
    """
    current_books: dict[str, Book] = {}
    try:
        async with asyncio.TaskGroup() as tasks:
            saver = tasks.create_task(
                sample_writer.save_books_periodically(
                    current_books, state.session_id, interval=settings.book_sample_interval,
                ),
                name=f"book-sampler-{state.session_id}",
            )
            try:
                await _receive_session(ws, writer, events, state, current_books, settings)
            finally:
                current_books.clear()
                saver.cancel()
    except ExceptionGroup as errors:
        # Keep the existing reconnect policy, prioritising a concurrent disk failure.
        for error in errors.exceptions:
            if not is_retryable(error):
                raise error from errors
        raise errors.exceptions[0] from errors


async def _receive_session(
    ws: ClientConnection, writer: RawMessageWriter, events: logging.Logger, state: SessionState,
    current_books: dict[str, Book], settings: IngestionSettings,
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
        current_books (dict[str, Book]): Shared validated books read by the
            sampling task on the same event loop. Updates commit without yielding.
        settings (IngestionSettings): Subscription and reception timeout settings.

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
    deadline = loop.time() + settings.startup_timeout
    pending_acks = set(settings.symbols)
    pending_snapshots = set(settings.symbols)
    subscription = {
        "method": "subscribe",
        "params": {"channel": "book", "symbol": settings.symbols, "depth": settings.depth, "snapshot": True},
        "req_id": 1,
    }
    async with asyncio.timeout(settings.startup_timeout):
        await ws.send(json.dumps(subscription))

    while True:
        timeout = settings.idle_timeout
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
                or result.get("symbol") not in settings.symbols
                or result.get("depth") != settings.depth
                or message.get("req_id") != 1
            ):
                raise ReconnectRequired("Unexpected subscription acknowledgement")
            pending_acks.discard(result["symbol"])
            events.info("Subscribed: %s, depth=%s", result["symbol"], settings.depth)
            for warning in result.get("warnings", []):
                events.warning("Subscription warning: %s", warning)
        elif message.get("channel") == "book":
            kind, data = message.get("type"), message.get("data")
            if kind not in ("snapshot", "update") or not isinstance(data, list) or not data:
                raise ReconnectRequired("Malformed book message")
            symbols = set()
            for item in data:
                if not isinstance(item, dict) or item.get("symbol") not in settings.symbols:
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
                        current_books[symbol] = snapshot.to_book(depth=settings.depth)
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


async def ingest(
    writer: RawMessageWriter, events: logging.Logger, sample_writer: ValidatedBookSampleWriter,
    settings: IngestionSettings,
) -> None:
    """Collect continuously, reconnecting after errors classified as temporary.

    Each attempt creates new session state with a fresh UUID that is included in
    every archived message. Failed sessions close before retrying with
    exponential backoff and bounded random jitter.
    The base delay resets only after a fully ready session has remained stable
    for the configured stable connection time. Every reconnect requests fresh snapshots and
    reports a data gap; missed historical updates are not recovered.

    Connection timeouts, keepalive intervals, message-size limits, and the
    receive-buffer high-water mark come from the ingestion settings.

    Args:
        writer (RawMessageWriter): Shared destination for timestamped raw messages.
        events (logging.Logger): Logger for attempts, failures, and retry delays.
        sample_writer (ValidatedBookSampleWriter): Shared sampled-book destination;
            each connection owns a separate, supervised sampling task.
        settings (IngestionSettings): Connection, subscription, sampling, and retry settings.

    Returns:
        None: Runs until cancellation or an unrecoverable error, without a
            normal return from the reconnection loop.

    Raises:
        Exception: A connection or collection error is classified as fatal,
            including subscription rejection or inability to archive messages.
        asyncio.CancelledError: Collection or a retry wait is cancelled.
    """
    delay = settings.min_retry_delay
    loop = asyncio.get_running_loop()
    while True:
        state = SessionState()
        try:
            events.info("Connecting to %s (session_id=%s)", settings.url, state.session_id)
            async with connect(
                settings.url, open_timeout=settings.open_timeout,
                ping_interval=settings.ping_interval, ping_timeout=settings.ping_timeout,
                close_timeout=settings.close_timeout, max_queue=settings.max_queue,
                max_size=settings.max_size,
                logger=events.getChild("websocket"),
            ) as ws:
                events.info("WebSocket connected (session_id=%s)", state.session_id)
                await collect_session(ws, writer, events, state, sample_writer, settings)
        except Exception as error:
            if not is_retryable(error):
                raise
            if state.ready_at is not None and loop.time() - state.ready_at >= settings.stable_connection_time:
                delay = settings.min_retry_delay
            wait = random.uniform(delay, min(settings.max_retry_delay, delay * 1.25))
            events.warning(
                "Connection interrupted (session_id=%s, %s: %s). Data gap; reconnecting in %.1fs "
                "and requesting fresh snapshots.", state.session_id, type(error).__name__, error, wait,
                exc_info=True,
            )
            await asyncio.sleep(wait)
            delay = min(settings.max_retry_delay, delay * 2)


async def main(config: AppConfig) -> None:
    """Open collection logs and run until the application cancels this component.

    Cancellation unwinds the connection context and lets protected writes finish.
    The application launcher owns signal handling and supervises both components.

    Args:
        config (AppConfig): Validated application settings loaded from YAML.

    Returns:
        None: No result is produced; normal operation continues until stopped.

    Raises:
        asyncio.CancelledError: Shutdown was requested; propagated after logging.
        Exception: Log setup fails or ingestion encounters an unrecoverable error.
            Ingestion failures are logged with their traceback before propagation.
    """
    with open_logs(config.logging.directory, config.logging) as (writer, sample_writer, events):
        try:
            await ingest(writer, events, sample_writer, config.ingestion)
        except asyncio.CancelledError:
            events.info("Stopped; closing the connection and flushing logs")
            raise
        except Exception:
            events.exception("Ingestion stopped after an unrecoverable error")
            raise
