import asyncio
import json
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import websockets

URL = "wss://ws.kraken.com/v2"
SYMBOLS = ["BTC/USD", "ETH/USD"]

Path("logs").mkdir(exist_ok=True)

logger = logging.getLogger("websocket_book_L2")
logger.setLevel(logging.INFO)
logger.propagate = False

handler = TimedRotatingFileHandler(
    Path("logs") / "kraken_book_L2.jsonl",
    when="m",
    interval=15,
    backupCount=0,
    encoding="utf-8",
    utc=True,
)

handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)

async def main():
    subscription = {
        "method": "subscribe",
        "params": {
            "channel": "book",
            "symbol": SYMBOLS,
            "depth": 25,
            "snapshot": True,
        }
    }

    async with websockets.connect(URL) as ws:
        await ws.send(json.dumps(subscription))

        async for raw_message in ws:
            message = json.loads(raw_message)

            if message.get("method") == "suscribe":
                if not message.get("success"):
                    raise RuntimeError(f"Subscription failed: {message.get('error')}")
                print("Suscribed:", message["result"]["symbol"])
            elif message.get("channel") == "book":
                logger.info("%s", message)
            # else:
            #     print(f"Unhandled message: {message}")
                    


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped.")
