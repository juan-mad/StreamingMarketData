"""Launch ingestion and archive conversion as one supervised application."""

import argparse
import asyncio
import logging
import signal
import sys
import time
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from streamingmarketdata.configuration import AppConfig, load_config
from streamingmarketdata.data_ingestion import main as run_ingestion
from streamingmarketdata.file_converter import main as run_converter


@contextmanager
def shutdown_signals() -> Iterator[None]:
    """Route SIGTERM to the application task, preserving worker cleanup.

    Repeated SIGTERM signals do not interrupt cancellation cleanup. Platforms
    without event-loop signal handlers still support Ctrl+C through asyncio.run.

    Args:
        None: Uses the currently running event loop and application task.

    Yields:
        None: SIGTERM handling remains active until the context exits.

    Raises:
        RuntimeError: There is no current application task.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    if task is None:
        raise RuntimeError("Shutdown handling requires an application task")

    def request_shutdown() -> None:
        """Cancel the application once so both components can finish cleanup.

        Args:
            None.

        Returns:
            None.
        """
        if not task.cancelling():
            task.cancel()

    previous_handler = signal.getsignal(signal.SIGTERM)
    installed = False
    try:
        try:
            loop.add_signal_handler(signal.SIGTERM, request_shutdown)
            installed = True
        except (NotImplementedError, RuntimeError):
            pass
        yield
    finally:
        if installed:
            loop.remove_signal_handler(signal.SIGTERM)
            signal.signal(signal.SIGTERM, previous_handler)


@contextmanager
def application_logging(config: AppConfig) -> Iterator[logging.Logger]:
    """Configure application diagnostics and close handlers after task cleanup.

    Args:
        config (AppConfig): Directory, file name, verbosity, and retention settings.

    Yields:
        logging.Logger: Logger shared with the converter through package ancestry.

    Raises:
        OSError: The log directory or diagnostic file cannot be opened.
    """
    config.logging.directory.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("streamingmarketdata")
    previous_level, previous_propagation = logger.level, logger.propagate
    logger.setLevel(config.logging.level)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(name)s: %(message)s")
    formatter.converter = time.gmtime
    try:
        with ExitStack() as stack:
            console = logging.StreamHandler()
            stack.callback(console.close)
            diagnostic = TimedRotatingFileHandler(
                config.logging.directory / config.logging.application_filename,
                when="midnight", backupCount=config.logging.diagnostic_backups,
                encoding="utf-8", utc=True,
            )
            stack.callback(diagnostic.close)
            for handler in (console, diagnostic):
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                stack.callback(logger.removeHandler, handler)
            yield logger
    finally:
        logger.setLevel(previous_level)
        logger.propagate = previous_propagation


async def run_application(config: AppConfig) -> None:
    """Run collection and conversion together until cancellation or fatal failure.

    Args:
        config (AppConfig): Validated settings shared by both application tasks.

    Returns:
        None: Runs until stopped; task cleanup completes before logs close.

    Raises:
        asyncio.CancelledError: Application shutdown was requested.
        ExceptionGroup: A component failed; its sibling is cancelled and awaited.
        OSError: Application logging cannot be initialized.
    """
    with application_logging(config) as logger, shutdown_signals():
        logger.info("Starting ingestion and file conversion")
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(run_ingestion(config), name="data-ingestion")
                tasks.create_task(run_converter(config), name="file-converter")
        except asyncio.CancelledError:
            logger.info("Application shutdown requested; both components have stopped")
            raise
        except Exception:
            logger.exception("Application stopped after an unrecoverable error")
            raise


def main(argv: Sequence[str] | None = None) -> None:
    """Load the selected YAML file and launch the combined application.

    Args:
        argv (Sequence[str] | None): Command-line arguments excluding the program
            name. None reads arguments from sys.argv.

    Returns:
        None: The application stopped following a user interruption.

    Raises:
        SystemExit: Argument parsing failed or an unrecoverable error occurred.
    """
    parser = argparse.ArgumentParser(description="Collect Kraken data and compress rotated archives.")
    parser.add_argument(
        "--config", type=Path,
        help="YAML configuration file (default: bundled config.yaml)",
    )
    arguments = parser.parse_args(argv)
    try:
        config = load_config(arguments.config)
        asyncio.run(run_application(config))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except Exception as error:
        print(f"Fatal application error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
