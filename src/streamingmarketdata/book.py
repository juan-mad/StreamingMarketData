import heapq
import binascii
from typing import Annotated, Any
from decimal import Decimal

from pydantic import AwareDatetime, BaseModel, Field

Price = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
Quantity = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
UpdateQuantity = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
Checksum = Annotated[int, Field(strict=True, ge=0, le=0xFFFFFFFF)]


class ChecksumFailureError(RuntimeError):
    """Signal a CRC32 mismatch requiring a fresh snapshot."""


class Update(BaseModel):
    """Validated price-level replacements, with zero quantities representing deletions."""

    symbol: str
    bids: dict[Price, UpdateQuantity]
    asks: dict[Price, UpdateQuantity]
    checksum: Checksum
    timestamp: AwareDatetime


class Book(BaseModel):
    """A checked order book containing only positive, finite price levels."""

    symbol: str
    bids: dict[Price, Quantity]
    asks: dict[Price, Quantity]
    checksum: Checksum
    timestamp: AwareDatetime
    depth: Annotated[int, Field(strict=True, gt=0)] = 25

    def model_post_init(self, context: Any) -> None:
        """Check subscribed depth and CRC32 after all fields have been validated.

        Args:
            context (Any): Optional Pydantic validation context; unused here.

        Returns:
            None.

        Raises:
            ValueError: Either side contains more levels than the subscribed depth.
            ChecksumFailureError: The top-ten checksum differs from the supplied value.
        """
        if len(self.bids) > self.depth or len(self.asks) > self.depth:
            raise ValueError("Book exceeds the subscribed depth")
        self.verify_checksum()

    def verify_checksum(self, target_checksum: int | None = None) -> None:
        """Compare the top-ten CRC32 with the supplied or stored checksum.

        Args:
            target_checksum (int | None): Expected unsigned checksum; None uses
                self.checksum. Zero is treated as an explicit checksum.

        Returns:
            None: Successful completion indicates a matching checksum.

        Raises:
            ChecksumFailureError: Computed and expected checksums differ.
        """
        validation_string = ""
        if target_checksum is None:
            target_checksum = self.checksum

        top_10_asks = heapq.nsmallest(10, self.asks.items())
        top_10_bids = heapq.nlargest(10, self.bids.items())

        for price, qty in top_10_asks:
            validation_string = (
                validation_string +
                format(price, "f").replace(".", "").lstrip("0") +
                format(qty, "f").replace(".", "").lstrip("0")
            )

        for price, qty in top_10_bids:
            validation_string = (
                validation_string +
                format(price, "f").replace(".", "").lstrip("0") +
                format(qty, "f").replace(".", "").lstrip("0")
            )

        computed_checksum = binascii.crc32(validation_string.encode("utf-8"))

        if target_checksum != computed_checksum:
            raise ChecksumFailureError(f"Checksum failure: expected {target_checksum}, computed {computed_checksum}")

    def apply_update(self, update: Update) -> None:
        """Validate and commit a staged update without changing the book on failure.

        Args:
            update (Update): Validated replacements and deletions for this symbol.

        Returns:
            None: On success, levels, checksum, and timestamp have been replaced.

        Raises:
            ValueError: The update belongs to another symbol.
            pydantic.ValidationError: A staged level or metadata field is invalid.
            ChecksumFailureError: The staged book fails checksum verification.
        """

        if self.symbol != update.symbol:
            raise ValueError(f"Snapshot symbol {self.symbol} does not match update symbol {update.symbol}")

        staging_bids = self.bids.copy()
        staging_asks = self.asks.copy()

        for price, qty in update.bids.items():
            if qty == Decimal("0"):
                staging_bids.pop(price, None)
            else:
                staging_bids[price] = qty

        if len(staging_bids) > self.depth:
            staging_bids = {price: staging_bids[price] for price in heapq.nlargest(self.depth, staging_bids.keys())}

        for price, qty in update.asks.items():
            if qty == Decimal("0"):
                staging_asks.pop(price, None)
            else:
                staging_asks[price] = qty

        if len(staging_asks) > self.depth:
            staging_asks = {price: staging_asks[price] for price in heapq.nsmallest(self.depth, staging_asks.keys())}

        try:
            staged_book = type(self).model_validate(self.model_dump() | {
                "bids": staging_bids,
                "asks": staging_asks,
                "checksum": update.checksum,
                "timestamp": update.timestamp,
            })
        except ChecksumFailureError as e:
            e.add_note("- failed during update")
            raise e

        self.bids = staged_book.bids
        self.asks = staged_book.asks
        self.checksum = staged_book.checksum
        self.timestamp = staged_book.timestamp
