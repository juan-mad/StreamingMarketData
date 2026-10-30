import datetime as dt
import heapq
import binascii
from typing import Any
from decimal import Decimal

from pydantic import BaseModel


class Update(BaseModel):
    symbol: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    checksum: int
    timestamp: dt.datetime


class Book(BaseModel):
    """ """
    symbol: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    checksum: int
    timestamp: dt.datetime

    def model_post_init(self, context=Any) -> None:
        self.verify_checksum()

    def verify_checksum(self, target_checksum=None):
        """Verify correct checksum"""
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
            raise ValueError(f"Checksum failure: expected {target_checksum}, computed {computed_checksum}")

    def apply_update(self, update: Update):
        """Updates the records of the snapshot with the given update, and verifies checksum."""

        # sanity check that symbols are matching
        if self.symbol != update.symbol:
            raise ValueError(f"Snapshot symbol {self.symbol} does not match update symbol {update.symbol}")

        staging_bids = self.bids.copy()
        staging_asks = self.asks.copy()

        # update bids
        for price, qty in update.bids.items():
            if qty == Decimal("0") and price in staging_bids:
                staging_bids.pop(price, None)
            else:
                staging_bids[price] = qty

        if len(staging_bids) > 25:
            staging_bids = {price: staging_bids[price] for price in heapq.nlargest(25, staging_bids.keys())}

        # update asks
        for price, qty in update.asks.items():
            if qty == Decimal("0") and price in staging_asks:
                staging_asks.pop(price, None)
            else:
                staging_asks[price] = qty

        if len(staging_asks) > 25:
            staging_asks = {price: staging_asks[price] for price in heapq.nsmallest(25, staging_asks.keys())}


        staged_book = self.model_copy(
            update={"bids": staging_bids, "asks": staging_asks, "checksum": update.checksum, "timestamp": update.timestamp}, deep=True,

        )
        staged_book.verify_checksum(target_checksum=update.checksum)

        # Finalize with update
        self.bids = staged_book.bids
        self.asks = staged_book.asks
        self.checksum = staged_book.checksum
        self.timestamp = staged_book.timestamp
