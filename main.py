"""
NY DOS Business Entity Scraper — Apify Actor entry point.
Uses NY Open Data SODA API — no browser, no proxy required.
"""

import asyncio
import json
import logging
import os

from apify_client import ApifyClient
from src.scraper import NYDOSScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def get_input() -> dict:
    token = os.environ.get("APIFY_TOKEN", "")
    kv_store_id = os.environ.get("APIFY_DEFAULT_KEY_VALUE_STORE_ID", "")
    input_key = os.environ.get("ACTOR_INPUT_KEY", "INPUT")

    if token and kv_store_id:
        try:
            record = ApifyClient(token).key_value_store(kv_store_id).get_record(input_key)
            if record and record.get("value"):
                return record["value"]
        except Exception as exc:
            log.warning(f"Could not read input from KV store: {exc}")

    local_input = os.path.join(
        os.environ.get("APIFY_LOCAL_STORAGE_DIR", "./apify_storage"),
        "key_value_stores", "default", f"{input_key}.json",
    )
    if os.path.exists(local_input):
        with open(local_input) as f:
            return json.load(f)
    return {}


def make_push_callback(token: str, dataset_id: str):
    client = ApifyClient(token) if token else None

    async def push(item: dict) -> None:
        if client and dataset_id:
            try:
                client.dataset(dataset_id).push_items([item])
            except Exception as exc:
                log.warning(f"Failed to push item: {exc}")
                log.info(f"RESULT: {json.dumps(item)}")
        else:
            log.info(f"RESULT: {json.dumps(item)}")

    return push


async def main() -> None:
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║   NY DOS Business Entity Scraper  — Starting     ║")
    log.info("╚══════════════════════════════════════════════════╝")

    actor_input = get_input()
    log.info(f"Input: {actor_input}")

    token = os.environ.get("APIFY_TOKEN", "")
    dataset_id = os.environ.get("APIFY_DEFAULT_DATASET_ID", "")
    push_callback = make_push_callback(token, dataset_id)

    scraper = NYDOSScraper(actor_input, push_callback, log)
    count = await scraper.run()

    log.info(f"✅ Finished. Total records scraped: {count}")


if __name__ == "__main__":
    asyncio.run(main())
