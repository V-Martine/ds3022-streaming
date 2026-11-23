"""Bluesky Jetstream -> Kafka topic bluesky_posts.
Had to change from Coinbase to Bluesky

Run:
    python -u producer.py

Adapted from the class producer and the worksheet's WebSocket pattern.
Uses certifi for TLS certificate verification.
"""

import asyncio
import json
import logging
import ssl
import time

import certifi
from tornado.httpclient import HTTPRequest
from tornado.websocket import websocket_connect

print("Loading Quix Streams; this may take a minute or two...", flush=True)
from quixstreams import Application


BROKER = "127.0.0.1:19092"
TOPIC = "bluesky_posts"

# Only request events for Bluesky posts.
WSS_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)

# Jetstream starts sending events without a subscription message.
SUBSCRIBE = None

RECONNECT_SECONDS = 5
FLUSH_INTERVAL_SECONDS = 5
FLUSH_TIMEOUT_SECONDS = 10

logger = logging.getLogger("bluesky_producer")


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        force=True,
    )
    logger.setLevel(logging.INFO)


def send(producer, payload):
    """Queue one event, keyed by the author's DID."""
    key = payload.get("did")

    if not key:
        logger.warning("Missing DID; sending without a key.")

    message = {
        "action": "live",
        "data": payload,
    }

    producer.produce(
        topic=TOPIC,
        key=key,
        value=json.dumps(message).encode("utf-8"),
    )


def flush_pending(producer):
    remaining = producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)

    if remaining:
        logger.warning(
            "%s records remain undelivered; check Kafka connection errors.",
            remaining,
        )

    return remaining


async def live(producer, ssl_context):
    queued = 0
    last_flush = time.monotonic()

    while True:
        ws = None

        try:
            logger.info("Connecting to Bluesky Jetstream...")

            request = HTTPRequest(
                url=WSS_URL,
                ssl_options=ssl_context,
                connect_timeout=20,
            )

            ws = await websocket_connect(
                request,
                ping_interval=15,
                max_message_size=10 * 1024 * 1024,
            )

            logger.info("Connected. Waiting for post events...")

            while True:
                text = await ws.read_message()

                if text is None:
                    logger.warning("Jetstream closed the connection.")
                    break

                try:
                    payload = json.loads(text)
                except (ValueError, TypeError):
                    logger.warning("Skipping invalid JSON.")
                    continue

                if not isinstance(payload, dict):
                    continue

                # Jetstream also has identity/account events.
                if payload.get("kind") != "commit":
                    continue

                commit = payload.get("commit") or {}
                if not isinstance(commit, dict):
                    continue

                if commit.get("collection") != "app.bsky.feed.post":
                    continue

                # Keep create, update, and delete events for this collection.
                send(producer, payload)
                queued += 1

                if queued <= 5 or queued % 1000 == 0:
                    logger.info(
                        "Queued %d events | DID=%s | operation=%s",
                        queued,
                        payload.get("did"),
                        commit.get("operation"),
                    )

                now = time.monotonic()

                # First-record delivery check, then periodic flushing.
                if queued == 1 or now - last_flush >= FLUSH_INTERVAL_SECONDS:
                    remaining = flush_pending(producer)

                    if queued == 1 and remaining == 0:
                        logger.info(
                            "First flush finished; verify the topic in T4."
                        )

                    last_flush = time.monotonic()

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("Bluesky streaming attempt failed.")

        finally:
            if ws is not None:
                ws.close()

        logger.info("Reconnecting in %d seconds...", RECONNECT_SECONDS)
        await asyncio.sleep(RECONNECT_SECONDS)


def main():
    configure_logging()

    logger.info("Starting file: %s", __file__)
    logger.info("Broker: %s | Topic: %s", BROKER, TOPIC)
    logger.info("Loading certificate bundle: %s", certifi.where())

    # Keep certificate and hostname verification enabled.
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    logger.info("Certificate bundle loaded.")

    app = Application(
        broker_address=BROKER,
        loglevel="INFO",
    )

    configure_logging()

    with app.get_producer() as producer:
        try:
            asyncio.run(live(producer, ssl_context))
        except KeyboardInterrupt:
            logger.info("Stopping producer...")
        finally:
            remaining = flush_pending(producer)
            logger.info("Stopped. Records still pending: %s", remaining)


if __name__ == "__main__":
    main()