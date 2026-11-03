"""Validate every Kraken price-level entry before collapsing duplicate prices."""

import re
from decimal import Decimal
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, Field, field_validator

from .book import Book, Checksum, Price, Quantity, Update, UpdateQuantity


class UpdateLevel(BaseModel):
    """An incoming positive price and a nonnegative replacement quantity."""

    price: Price
    qty: UpdateQuantity

    @field_validator("price", "qty", mode="before")
    @classmethod
    def require_json_number(cls, value: object) -> Decimal | int:
        """Accept decoded JSON numbers without accepting strings or booleans.

        Args:
            value (object): Field value decoded using parse_float=Decimal.

        Returns:
            Decimal | int: Unmodified numeric value, preserving trailing zeros.

        Raises:
            ValueError: The value is not a Decimal or a non-boolean integer.
        """
        if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
            raise ValueError("Prices and quantities must be JSON numbers")
        return value


class SnapshotLevel(UpdateLevel):
    """An incoming snapshot level whose price and quantity are strictly positive."""

    qty: Quantity


class BookData(BaseModel):
    """Metadata shared by incoming snapshots and updates."""

    symbol: Annotated[str, Field(strict=True, min_length=1)]
    checksum: Checksum
    timestamp: AwareDatetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def require_rfc3339(cls, value: object) -> str:
        """Require a timestamp string with a date, time, and timezone.

        Args:
            value (object): Incoming timestamp field, before datetime conversion.

        Returns:
            str: Original timestamp for Pydantic to validate as a calendar date.

        Raises:
            ValueError: The value is not an RFC3339-shaped string with a timezone.
        """
        pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
        if not isinstance(value, str) or re.fullmatch(pattern, value, re.IGNORECASE) is None:
            raise ValueError("Timestamp must be an RFC3339 string with a timezone")
        return value


class SnapshotData(BookData):
    """A snapshot whose every level is validated before book construction."""

    bids: list[SnapshotLevel]
    asks: list[SnapshotLevel]

    def to_book(self, depth: int) -> Book:
        """Build and checksum a snapshot without hiding duplicate price levels.

        Args:
            depth (int): Maximum subscribed number of levels on either side.

        Returns:
            Book: Validated order book containing the snapshot's exact decimals.

        Raises:
            ValueError: A snapshot contains duplicate prices on the same side.
            pydantic.ValidationError: Depth, metadata, or a book field is invalid.
            ChecksumFailureError: The snapshot's computed CRC32 does not match.
        """
        bids = {level.price: level.qty for level in self.bids}
        asks = {level.price: level.qty for level in self.asks}
        if len(bids) != len(self.bids) or len(asks) != len(self.asks):
            raise ValueError("Snapshot contains duplicate price levels")
        return Book(
            symbol=self.symbol, bids=bids, asks=asks, checksum=self.checksum,
            timestamp=self.timestamp, depth=depth,
        )


class UpdateData(BookData):
    """An update retaining every level until all entries have been validated."""

    bids: list[UpdateLevel]
    asks: list[UpdateLevel]

    def to_update(self) -> Update:
        """Convert validated levels into replacements, keeping each price's last value.

        Args:
            None: All input fields have already been validated on this instance.

        Returns:
            Update: Validated replacements and deletions preserving decimal text.

        Raises:
            pydantic.ValidationError: A resulting update field is invalid.
        """
        return Update(
            symbol=self.symbol,
            bids={level.price: level.qty for level in self.bids},
            asks={level.price: level.qty for level in self.asks},
            checksum=self.checksum, timestamp=self.timestamp,
        )
