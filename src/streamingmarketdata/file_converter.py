import asyncio
from pathlib import Path
import gzip
import logging
import shutil

WAIT = 10
LOG_DIRECTORY = Path("logs")
logger = logging.getLogger(__name__)


def compress_and_delete_raw_file(raw_file_path: Path, output_dir: Path) -> None:
    """Compress a raw file and delete it only after compression succeeds.

    Args:
        raw_file_path (Path): Raw file to compress.
        output_dir (Path): Directory receiving the gzip file.

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


async def main(input_dir: Path, output_dir: Path) -> None:
    """Continuously compress raw files, recovering from processing errors.

    Args:
        input_dir (Path): Directory scanned for raw message files.
        output_dir (Path): Directory receiving compressed files.

    Returns:
        None: Runs until interrupted or cancelled.
    """
    logger.info("File converter started: input=%s, output=%s", input_dir, output_dir)
    while True:
        await asyncio.sleep(WAIT)
        logger.info("Checking %s for raw message files", input_dir)
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
            raw_message_files = list(input_dir.glob("kraken_ws_raw.jsonl.*"))

            async with asyncio.TaskGroup() as tg:
                for file in raw_message_files:
                    tg.create_task(asyncio.to_thread(compress_and_delete_raw_file, file, output_dir))
        except Exception:
            logger.exception("Conversion scan failed; will retry in %s seconds", WAIT)


if __name__ == "__main__":
    LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=LOG_DIRECTORY / "file_converter.log",
        filemode="a",
        encoding="utf-8",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    input_dir = Path("./input_dir")
    output_dir = Path("./output_dir")

    try:
        asyncio.run(main(input_dir, output_dir))
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("File converter stopped by cancellation")
