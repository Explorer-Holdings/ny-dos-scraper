"""
NY DOS Business Entity Scraper — Apify Actor entry point.
"""

import asyncio
from apify import Actor
from src.scraper import NYDOSScraper


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        Actor.log.info("╔══════════════════════════════════════════════════╗")
        Actor.log.info("║   NY DOS Business Entity Scraper  — Starting     ║")
        Actor.log.info("╚══════════════════════════════════════════════════╝")
        Actor.log.info(f"Input: {actor_input}")

        scraper = NYDOSScraper(actor_input)
        count = await scraper.run()

        Actor.log.info(f"✅ Finished. Total records scraped: {count}")


if __name__ == "__main__":
    asyncio.run(main())
