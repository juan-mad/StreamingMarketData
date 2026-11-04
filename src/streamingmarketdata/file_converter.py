"""Compress closed raw archives using the application's shared configuration."""

import asyncio
import gzip
import logging
import shutil
from pathlib import Path

from streamingmarketdata.configuration import AppConfig


def compress_and_delete_raw_file(
    raw_file_path: Path, output_dir: Path, logger: logging.Logger,
) -> None:
    """Compress a raw file and delete it only after compression succeeds.

    Args:
        raw_file_path (Path): Raw file to compress.
        output_dir (Path): Directory receiving the gzip file.
        logger (logging.Logger): Shared application logger for progress and failures.

    Returns:
        None: Failures are logged and the raw file is retained for retry.
    """
    try:
        output_file = output_dir / f"{raw_file_path.name}.gz"
        logger.info("Compressing %s", raw_file_path)
        with open(raw_file_path, "rb") as f_in:
            with gzip.open(output_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info("Deleting %s", raw_file_path)
        raw_file_path.unlink()
    except Exception:
        logger.exception("Failed to process %s; will retry on the next scan", raw_file_path)


async def compress_file(raw_file_path: Path, output_dir: Path, logger: logging.Logger) -> None:
    """Wait for a compression worker to finish even when shutdown is requested.

    Args:
        raw_file_path (Path): Closed raw archive to compress and delete.
        output_dir (Path): Directory receiving the gzip file.
        logger (logging.Logger): Application logger kept open until work finishes.

    Returns:
        None: Compression completed or its failure was logged for later retry.

    Raises:
        asyncio.CancelledError: Shutdown was requested; the worker finished first.
    """
    work = asyncio.create_task(asyncio.to_thread(
        compress_and_delete_raw_file, raw_file_path, output_dir, logger,
    ))
    try:
        await asyncio.shield(work)
    except asyncio.CancelledError:
        await work
        raise


async def main(config: AppConfig) -> None:
    """Continuously compress raw files, recovering from processing errors.

    Args:
        config (AppConfig): Shared log directory, raw file name, output directory,
            and scan interval loaded from YAML.

    Returns:
        None: Runs until interrupted or cancelled.

    Raises:
        asyncio.CancelledError: Shutdown was requested; active workers are awaited.
    """
    input_dir = config.logging.directory
    output_dir = config.conversion.output_directory
    logger = logging.getLogger(__name__)
    logger.info("File converter started: input=%s, output=%s", input_dir, output_dir)
    while True:
        await asyncio.sleep(config.conversion.scan_interval)
        logger.info("Checking %s for raw message files", input_dir)
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            raw_message_files = [
                file for file in input_dir.glob(f"{config.logging.raw_filename}.*")
                if file.is_file() and not file.name.endswith(".gz")
            ]

            async with asyncio.TaskGroup() as tg:
                for file in raw_message_files:
                    tg.create_task(compress_file(file, output_dir, logger))
        except Exception:
            logger.exception("Conversion scan failed; will retry in %s seconds", config.conversion.scan_interval)
