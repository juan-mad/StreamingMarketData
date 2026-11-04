"""Load and validate application settings from YAML without import-time I/O."""

from pathlib import Path
from typing import Annotated, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

PositiveSeconds = Annotated[float, Field(gt=0, allow_inf_nan=False)]
PositiveInteger = Annotated[int, Field(strict=True, gt=0)]
FileName = Annotated[str, Field(pattern=r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")]


class Settings(BaseModel):
    """Reject unknown settings and prevent changes after validation."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestionSettings(Settings):
    """Configure subscriptions, sampling, connection limits, and retry delays.

    All timeout, interval, and delay values are expressed in seconds.
    """

    url: str
    symbols: Annotated[tuple[str, ...], Field(min_length=1)]
    depth: PositiveInteger
    startup_timeout: PositiveSeconds
    idle_timeout: PositiveSeconds
    min_retry_delay: PositiveSeconds
    max_retry_delay: PositiveSeconds
    stable_connection_time: PositiveSeconds
    book_sample_interval: PositiveSeconds
    open_timeout: PositiveSeconds
    ping_interval: PositiveSeconds
    ping_timeout: PositiveSeconds
    close_timeout: PositiveSeconds
    max_queue: PositiveInteger
    max_size: PositiveInteger

    @model_validator(mode="after")
    def check_subscription_and_retries(self) -> Self:
        """Reject unusable connection settings and inconsistent retry bounds.

        Args:
            None: Reads this model's validated fields.

        Returns:
            Self: This settings instance when its fields are consistent.

        Raises:
            ValueError: The URL, symbols, or retry bounds are invalid.
        """
        if not self.url.startswith(("ws://", "wss://")):
            raise ValueError("url must start with ws:// or wss://")
        if any(not symbol.strip() for symbol in self.symbols):
            raise ValueError("symbols must contain nonempty trading pairs")
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("symbols must not contain duplicates")
        if self.max_retry_delay < self.min_retry_delay:
            raise ValueError("max_retry_delay must be at least min_retry_delay")
        return self


class LoggingSettings(Settings):
    """Configure shared storage, file names, retention, and diagnostic verbosity."""

    directory: Path
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    application_filename: FileName
    events_filename: FileName
    raw_filename: FileName
    samples_filename: FileName
    rotation_minutes: PositiveInteger
    diagnostic_backups: Annotated[int, Field(strict=True, ge=0)]

    @model_validator(mode="after")
    def check_distinct_filenames(self) -> Self:
        """Prevent diagnostic and data files from overwriting one another.

        Args:
            None: Reads this model's validated file names.

        Returns:
            Self: This settings instance if its file names are distinct.

        Raises:
            ValueError: File names overlap, including rotated raw-file names.
        """
        names = (
            self.application_filename, self.events_filename,
            self.raw_filename, self.samples_filename,
        )
        if len(set(names)) != len(names):
            raise ValueError("logging file names must be distinct")
        if any(name.startswith(f"{self.raw_filename}.") for name in names if name != self.raw_filename):
            raise ValueError("other log files must not match the rotated raw-file prefix")
        return self


class ConversionSettings(Settings):
    """Configure compressed-file storage and the scan interval in seconds."""

    output_directory: Path
    scan_interval: PositiveSeconds


class AppConfig(Settings):
    """Contain the complete configuration shared by both application tasks."""

    ingestion: IngestionSettings
    logging: LoggingSettings
    conversion: ConversionSettings


def load_config(path: Path | None = None) -> AppConfig:
    """Read YAML settings and resolve storage paths relative to the YAML file.

    Args:
        path (Path | None): YAML file to read. None selects the config.yaml
            distributed alongside this module.

    Returns:
        AppConfig: Validated settings with absolute storage-directory paths.

    Raises:
        OSError: The configuration file cannot be read.
        yaml.YAMLError: The file contains invalid YAML.
        ValueError: Required settings are missing, invalid, or unknown.
    """
    config_path = (path if path is not None else Path(__file__).with_name("config.yaml")).resolve()
    with config_path.open(encoding="utf-8") as source:
        config = AppConfig.model_validate(yaml.safe_load(source))
    log_directory = (config_path.parent / config.logging.directory).resolve()
    output_directory = (config_path.parent / config.conversion.output_directory).resolve()
    return config.model_copy(update={
        "logging": config.logging.model_copy(update={"directory": log_directory}),
        "conversion": config.conversion.model_copy(update={"output_directory": output_directory}),
    })
