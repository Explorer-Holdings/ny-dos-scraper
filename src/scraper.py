"""
NY DOS Business Entity Scraper — Core Scraper.

Strategy
--------
The NY DOS Public Inquiry site (https://apps.dos.ny.gov/publicInquiry/) is a
Vue.js SPA protected by a WAF that blocks datacenter IPs.  All direct API
calls return 403 "Host not in allowlist".

We therefore:
  1. Launch Playwright (Chromium) with an Apify residential proxy so the
     browser's outbound IP is a real residential address.
  2. Intercept every JSON response from the SPA's XHR/fetch calls so we can
     discover and reuse the backend REST endpoints for subsequent requests
     (faster than full page renders for each entity).
  3. Fall back to full DOM scraping if the intercepted API shape changes.

Output schema (matches parseforge/ny-business-entity-scraper):
  dosId, entityName, entityType, status, county, jurisdiction, fictName,
  dateFiled, effectiveDate, documentType,
  principalOfficeName, principalOfficeAddr1, principalOfficeCity,
  principalOfficeState, principalOfficeZip,
  registeredAgentName, registeredAgentAddr1, registeredAgentCity,
  registeredAgentState, registeredAgentZip,
  chiefOfficerName, filingHistory, displayNameHistory,
  mergerHistory, assumedNameHistory, url, scrapedAt
"""

from __future__ import annotations

import asyncio
import json
import re
import string
from datetime import datetime, date
from typing import Any
from urllib.parse import urlencode, urljoin

# apify SDK removed — using apify-client directly
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Response,
    async_playwright,
)

# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://apps.dos.ny.gov/publicInquiry/"
ENTITY_SEARCH_URL = urljoin(BASE_URL, "EntitySearch")
ENTITY_DETAIL_HASH = "https://apps.dos.ny.gov/publicInquiry/#DOS-{dos_id}"

# The site limits results to 500 per query
SITE_RESULT_LIMIT = 500

# Delay between page navigations (seconds) to be polite and avoid rate-limits
NAV_DELAY = 1.2

# Playwright launch options
VIEWPORT = {"width": 1920, "height": 1080}
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _camel(text: str) -> str:
    """Convert a label string like 'Date Filed' → 'dateFiled'."""
    words = re.split(r"[\s\-_/()+#]+", text.strip())
    if not words:
        return text.lower()
    result = words[0].lower()
    for w in words[1:]:
        if w:
            result += w[0].upper() + w[1:].lower()
    return result


def _parse_date(raw: str) -> str | None:
    """Try common date formats; return ISO string or None."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _in_range(filing_str: str, start: str | None, end: str | None) -> bool:
    """Return True if *filing_str* falls within [start, end]."""
    parsed = _parse_date(filing_str)
    if parsed is None:
        return True  # can't parse → include it
    if start and parsed < start:
        return False
    if end and parsed > end:
        return False
    return True


def _clean(text: str | None) -> str:
    """Strip whitespace and collapse internal runs."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


# ── Main Scraper Class ────────────────────────────────────────────────────────

class NYDOSScraper:
    """Playwright-based scraper for the NY DOS Public Inquiry portal."""

    def __init__(self, input_data: dict[str, Any], push_callback, logger) -> None:
        # Search parameters
        self.search_by: str = input_data.get("searchBy", "entityName")
        self.name_search: str = input_data.get("nameSearch", "").strip()
        self.active_only: bool = input_data.get("nameType", False)
        self.search_type: str = input_data.get("searchType", "BEGINS_WITH")
        self.entity_type: str = input_data.get("entityType", "")
        # Date range (ISO strings or None)
        self.start_date: str | None = input_data.get("startDate") or None
        self.end_date: str | None = input_data.get("endDate") or None
        # Extra filters (applied client-side after search)
        self.county_filter: str = input_data.get("county", "").strip().upper()
        self.status_filter: str = input_data.get("statusFilter", "").strip().upper()
        # Behaviour
        self.scrape_details: bool = input_data.get("scrapeDetails", True)
        self.max_results: int = int(input_data.get("maxResults", 500))
        self.proxy_config_input: dict = input_data.get("proxyConfiguration", {})

        # Internal state
        self._count: int = 0
        self._api_base: str | None = None          # discovered REST endpoint base
        self._api_cookies: dict[str, str] = {}    # session cookies for direct calls
        self._api_headers: dict[str, str] = {}    # extra headers for direct calls
        self._push = push_callback
        self.log = logger

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self) -> int:
        """Launch the browser and orchestrate the scrape."""
        async with async_playwright() as pw:
            proxy_config = await self._get_proxy_url()
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
                proxy=proxy_config,
            )
            try:
                await self._scrape(browser)
            finally:
                await browser.close()

        return self._count

    # ── Proxy setup ───────────────────────────────────────────────────────────

    async def _get_proxy_url(self) -> dict | None:
        """
        Build proxy config for Playwright.
        RESIDENTIAL requires a paid Apify plan — falls back to no proxy so the
        scraping logic can be verified. Switch apifyProxyGroups to ['SHADER']
        for free datacenter proxies if the site blocks direct connections.
        """
        import os
        proxy_password = os.environ.get("APIFY_PROXY_PASSWORD", "")
        use_proxy = self.proxy_config_input.get("useApifyProxy", False)
        groups = self.proxy_config_input.get("apifyProxyGroups", [])

        # Skip proxy entirely if not configured or no password
        if not use_proxy or not proxy_password or not groups:
            self.log.info("Running WITHOUT proxy — direct connection.")
            return None

        # RESIDENTIAL requires paid plan; SHADER is free datacenter
        group_str = "+".join(groups)
        if "RESIDENTIAL" in groups:
            self.log.warning(
                "RESIDENTIAL proxy requires a paid Apify plan. "
                "If this fails, change apifyProxyGroups to ['SHADER'] in the input."
            )

        proxy = {
            "server": "http://proxy.apify.com:8000",
            "username": f"groups-{group_str}",
            "password": proxy_password,
        }
        self.log.info(f"Using Apify proxy: groups={group_str}")
        return proxy

    # ── Browser context factory ───────────────────────────────────────────────

    async def _new_context(self, browser: Browser) -> BrowserContext:
        ctx = await browser.new_context(
            viewport=VIEWPORT,
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "DNT": "1",
            },
        )
        # Hide webdriver flag
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        return ctx

    # ── Top-level orchestration ───────────────────────────────────────────────

    async def _scrape(self, browser: Browser) -> None:
        """Decide whether to do a single search or an alphabetical sweep."""
        # Determine the set of search terms to use
        if self.search_by == "dosId" and self.name_search:
            # Single DOS-ID lookup
            await self._scrape_with_terms(browser, [self.name_search])

        elif self.name_search:
            # Normal name / assumed-name search
            await self._scrape_with_terms(browser, [self.name_search])

        else:
            # Broad sweep: search A–Z and 0–9 to capture everything,
            # then apply date/county/status filters client-side.
            self.log.info(
                "No name search provided — running alphabetical sweep (A–Z, 0–9). "
                "This is slow; consider narrowing with nameSearch."
            )
            terms = list(string.ascii_uppercase) + list(string.digits)
            await self._scrape_with_terms(browser, terms)

    async def _scrape_with_terms(
        self, browser: Browser, terms: list[str]
    ) -> None:
        """Run one or more search terms sequentially, collecting results."""
        seen_dos_ids: set[str] = set()

        for term in terms:
            if self._count >= self.max_results:
                break

            self.log.info(
                f"Searching '{term}' | type={self.search_type} | "
                f"active_only={self.active_only} | entity_type={self.entity_type!r}"
            )

            ctx = await self._new_context(browser)
            page = await ctx.new_page()

            # ── Intercept JSON API responses ──────────────────────────────
            captured: list[dict] = []

            async def on_response(resp: Response) -> None:
                """Capture any JSON list returned by the SPA's backend."""
                if resp.status != 200:
                    return
                ct = resp.headers.get("content-type", "")
                if "json" not in ct:
                    return
                try:
                    body = await resp.json()
                    if isinstance(body, list) and body:
                        first = body[0]
                        if isinstance(first, dict) and any(
                            k in first
                            for k in ("dosId", "entityName", "name", "id", "status")
                        ):
                            captured.append({"url": resp.url, "data": body})
                            if not self._api_base:
                                # Strip query string to get base
                                self._api_base = re.sub(r"\?.*", "", resp.url)
                                self.log.info(
                                    f"Discovered API endpoint: {self._api_base}"
                                )
                except Exception:
                    pass

            page.on("response", on_response)

            try:
                search_results = await self._do_search(page, term)

                # If we captured raw API data, merge it (richer than DOM parse)
                if captured:
                    raw_list = captured[-1]["data"]
                    # Normalise field names from whatever the API returns
                    search_results = [self._normalise_api_record(r) for r in raw_list]

                self.log.info(f"  → {len(search_results)} rows found for '{term}'")

                for entity in search_results:
                    if self._count >= self.max_results:
                        break

                    dos_id = entity.get("dosId", "")
                    if dos_id in seen_dos_ids:
                        continue
                    seen_dos_ids.add(dos_id)

                    # Client-side filters
                    if not self._passes_filters(entity):
                        continue

                    # Optionally fetch full detail
                    if self.scrape_details and dos_id:
                        detail = await self._fetch_entity_detail(page, dos_id)
                        entity.update(detail)

                    entity["url"] = ENTITY_DETAIL_HASH.format(dos_id=dos_id)
                    entity["scrapedAt"] = datetime.utcnow().isoformat() + "Z"

                    await self._push(entity)
                    self._count += 1

                    if self._count % 25 == 0:
                        self.log.info(f"  ✔ {self._count} records saved so far…")

                    await asyncio.sleep(NAV_DELAY)

            except Exception as exc:
                self.log.error(f"Error searching for '{term}': {exc}")

            finally:
                await page.close()
                await ctx.close()

    # ── Search form interaction ───────────────────────────────────────────────

    async def _do_search(self, page: Page, term: str) -> list[dict]:
        """
        Navigate to the NY DOS search page, fill the form, submit, and return results.
        Uses multiple strategies with debug logging to handle the Vue.js SPA.
        """
        # The SPA routes search through the hash — go directly to search view
        # Navigate to root — Vue router shows the search form by default
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)

        # Wait for Vue to mount — poll for any input element to appear
        try:
            await page.wait_for_selector("input, select, button", timeout=20_000)
        except Exception:
            self.log.warning("Timed out waiting for form elements — dumping page state")

        await asyncio.sleep(3)  # Extra wait for Vue reactivity

        # ── Debug: log what's on the page ────────────────────────────────
        page_title = await page.title()
        page_url = page.url
        self.log.info(f"Page title: {page_title!r}  URL: {page_url}")

        inputs = await page.locator("input").count()
        selects = await page.locator("select").count()
        buttons = await page.locator("button").count()
        self.log.info(f"Found: {inputs} inputs, {selects} selects, {buttons} buttons")

        # Log all button texts to find the search button
        for i in range(min(buttons, 10)):
            try:
                txt = await page.locator("button").nth(i).text_content()
                self.log.info(f"  button[{i}]: {txt!r}")
            except Exception:
                pass

        # Log all input types/placeholders
        for i in range(min(inputs, 10)):
            try:
                el = page.locator("input").nth(i)
                itype = await el.get_attribute("type") or "text"
                iname = await el.get_attribute("name") or ""
                iid = await el.get_attribute("id") or ""
                iph = await el.get_attribute("placeholder") or ""
                self.log.info(f"  input[{i}]: type={itype} id={iid!r} name={iname!r} ph={iph!r}")
            except Exception:
                pass

        # ── Save a screenshot to KV store for debugging ───────────────────
        try:
            import os
            from apify_client import ApifyClient
            screenshot_bytes = await page.screenshot(full_page=True)
            token = os.environ.get("APIFY_TOKEN", "")
            kv_id = os.environ.get("APIFY_DEFAULT_KEY_VALUE_STORE_ID", "")
            if token and kv_id:
                ApifyClient(token).key_value_store(kv_id).set_record(
                    "debug_screenshot.png", screenshot_bytes, content_type="image/png"
                )
                self.log.info("Debug screenshot saved to key-value store as 'debug_screenshot.png'")
        except Exception as exc:
            self.log.debug(f"Screenshot save failed: {exc}")

        # ── Strategy 1: try select + text input + radio + button ──────────
        # Search By select (first select on page)
        search_by_label_map = {
            "entityName": "Entity Name",
            "dosId": "DOS ID#",
            "assumedName": "Assumed Name",
            "assumedNameId": "Assumed Name ID#",
        }
        desired_label = search_by_label_map.get(self.search_by, "Entity Name")
        await self._try_select(
            page,
            selectors=["select >> nth=0"],
            value=self.search_by,
            label=desired_label,
        )
        await asyncio.sleep(0.5)

        # Name input — first visible text input
        filled = await self._try_fill(
            page,
            selectors=[
                "input[type='text']",
                "input:not([type='radio']):not([type='checkbox']):not([type='submit'])",
            ],
            value=term,
        )
        self.log.info(f"Name field filled: {filled}")
        await asyncio.sleep(0.3)

        # Name Type radio: Active Only vs All
        name_type_target = "Active" if self.active_only else "All"
        try:
            radio = page.get_by_label(name_type_target, exact=False)
            if await radio.count():
                await radio.first.click()
                self.log.info(f"Clicked name type radio: {name_type_target}")
        except Exception:
            pass

        # Search Type radio
        search_type_label_map = {
            "BEGINS_WITH": "Begins With",
            "CONTAINS": "Contains",
            "SOUNDS_LIKE": "Sounds Like",
        }
        st_label = search_type_label_map.get(self.search_type, "Begins With")
        try:
            st_radio = page.get_by_label(st_label, exact=False)
            if await st_radio.count():
                await st_radio.first.click()
                self.log.info(f"Clicked search type radio: {st_label}")
        except Exception:
            pass

        # Entity Type (second select if present)
        if self.entity_type:
            await self._try_select(
                page,
                selectors=["select >> nth=1", "select >> nth=-1"],
                value=self.entity_type,
                label=self.entity_type,
            )
            await asyncio.sleep(0.3)

        # ── Click Search button ───────────────────────────────────────────
        clicked = False
        # Try every button and click the first one that looks like "search"
        for i in range(min(buttons, 10)):
            try:
                btn = page.locator("button").nth(i)
                txt = (await btn.text_content() or "").strip().lower()
                if "search" in txt or txt == "":
                    await btn.click()
                    self.log.info(f"Clicked button[{i}]: {txt!r}")
                    clicked = True
                    break
            except Exception:
                pass

        if not clicked:
            # Try input[type=submit] or any button
            for sel in ["input[type='submit']", "button[type='submit']", "button >> nth=0"]:
                try:
                    el = page.locator(sel).first
                    if await el.count():
                        await el.click()
                        clicked = True
                        self.log.info(f"Clicked via selector: {sel}")
                        break
                except Exception:
                    pass

        if not clicked:
            self.log.warning("No button found — pressing Enter")
            await page.keyboard.press("Enter")

        # ── Wait for results ──────────────────────────────────────────────
        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        await asyncio.sleep(3)

        # Log post-search state
        post_buttons = await page.locator("button").count()
        post_inputs = await page.locator("input").count()
        tables = await page.locator("table").count()
        rows = await page.locator("tr").count()
        self.log.info(
            f"After search: {post_buttons} buttons, {post_inputs} inputs, "
            f"{tables} tables, {rows} table rows"
        )

        return await self._parse_results_table(page)

    # ── DOM table parser ──────────────────────────────────────────────────────

    async def _parse_results_table(self, page: Page) -> list[dict]:
        """
        Extract rows from the results table.

        Known column order (NY DOS site):
            0  Entity Name  (link)
            1  DOS ID#
            2  Assumed Name ID#
            3  Status
            4  Entity Type
            5  Date of First Filing
            6  County
        """
        results: list[dict] = []

        # Find the table
        table = page.locator("table").first
        if not await table.count():
            # Some SPA states render a list instead of a table
            table = page.locator("[role='table'], .results, .entity-list").first

        if not await table.count():
            self.log.warning("No results table found on page.")
            return results

        rows = table.locator("tbody tr, [role='row']")
        row_count = min(await rows.count(), SITE_RESULT_LIMIT)

        for i in range(row_count):
            row = rows.nth(i)
            cells = row.locator("td, [role='cell']")
            n = await cells.count()
            if n == 0:
                continue

            entity: dict[str, Any] = {}
            try:
                # Entity Name (col 0) — grab link text and href
                name_link = cells.nth(0).locator("a").first
                if await name_link.count():
                    entity["entityName"] = _clean(await name_link.text_content())
                    href = await name_link.get_attribute("href") or ""
                    # Extract DOS ID from href if present (#DOS-XXXX or ?dosId=XXXX)
                    m = re.search(r"DOS-(\d+)|dosId=(\d+)", href, re.I)
                    if m:
                        entity["dosId"] = m.group(1) or m.group(2)
                else:
                    entity["entityName"] = _clean(await cells.nth(0).text_content())

                if n > 1 and not entity.get("dosId"):
                    entity["dosId"] = _clean(await cells.nth(1).text_content())
                elif n > 1 and entity.get("dosId"):
                    pass  # already extracted from href
                else:
                    entity.setdefault("dosId", "")

                entity["assumedNameId"] = _clean(await cells.nth(2).text_content()) if n > 2 else ""
                entity["status"] = _clean(await cells.nth(3).text_content()) if n > 3 else ""
                entity["entityType"] = _clean(await cells.nth(4).text_content()) if n > 4 else ""
                entity["dateFiled"] = _parse_date(await cells.nth(5).text_content()) or _clean(await cells.nth(5).text_content()) if n > 5 else ""
                entity["county"] = _clean(await cells.nth(6).text_content()) if n > 6 else ""

                if entity.get("entityName"):
                    results.append(entity)
            except Exception as exc:
                self.log.debug(f"Row {i} parse error: {exc}")

        return results

    # ── Entity detail page ────────────────────────────────────────────────────

    async def _fetch_entity_detail(self, page: Page, dos_id: str) -> dict:
        """
        Navigate to an entity's detail page and extract every available field.

        The SPA renders the detail at the hash-URL:
            https://apps.dos.ny.gov/publicInquiry/#DOS-<dosId>
        """
        detail_url = ENTITY_DETAIL_HASH.format(dos_id=dos_id)
        detail: dict[str, Any] = {}

        try:
            await page.goto(detail_url, wait_until="networkidle", timeout=45_000)
            await asyncio.sleep(1.5)

            # ── Core entity fields ────────────────────────────────────────
            detail.update(await self._extract_kv_pairs(page))

            # ── Address blocks ────────────────────────────────────────────
            detail.update(await self._extract_address_blocks(page))

            # ── Filing History tab ────────────────────────────────────────
            detail["filingHistory"] = await self._click_tab_and_scrape_table(
                page, tab_name_pattern=re.compile(r"filing\s*history", re.I)
            )

            # ── Display Name History tab ──────────────────────────────────
            detail["displayNameHistory"] = await self._click_tab_and_scrape_table(
                page, tab_name_pattern=re.compile(r"display\s*name", re.I)
            )
            # Most recent document type comes from first filing history row
            if detail["filingHistory"]:
                detail.setdefault(
                    "documentType",
                    detail["filingHistory"][0].get("documentType", ""),
                )
                detail.setdefault(
                    "effectiveDate",
                    detail["filingHistory"][0].get("effectiveDate", ""),
                )

            # ── Merger History tab ────────────────────────────────────────
            detail["mergerHistory"] = await self._click_tab_and_scrape_table(
                page, tab_name_pattern=re.compile(r"merger", re.I)
            )

            # ── Assumed Name History tab ──────────────────────────────────
            detail["assumedNameHistory"] = await self._click_tab_and_scrape_table(
                page, tab_name_pattern=re.compile(r"assumed\s*name", re.I)
            )

        except Exception as exc:
            self.log.warning(f"Detail page error for DOS ID {dos_id}: {exc}")

        return detail

    async def _extract_kv_pairs(self, page: Page) -> dict:
        """Extract key-value pairs from definition lists and labelled spans."""
        fields: dict[str, str] = {}

        # Strategy 1: <dl> / <dt> / <dd>
        dts = page.locator("dt")
        dds = page.locator("dd")
        if await dts.count() == await dds.count() and await dts.count() > 0:
            for i in range(await dts.count()):
                k = _clean(await dts.nth(i).text_content()).rstrip(":")
                v = _clean(await dds.nth(i).text_content())
                if k:
                    fields[_camel(k)] = v

        # Strategy 2: <th> / <td> single-row summary tables
        ths = page.locator("table th")
        if await ths.count():
            for i in range(await ths.count()):
                th = ths.nth(i)
                k = _clean(await th.text_content()).rstrip(":")
                # sibling td
                td = th.locator("xpath=following-sibling::td[1]")
                if await td.count():
                    v = _clean(await td.text_content())
                    if k:
                        fields[_camel(k)] = v

        # Strategy 3: .field-label / .field-value patterns
        labels = page.locator(
            "[class*='label' i], [class*='field-name' i], [class*='key' i]"
        )
        for i in range(await labels.count()):
            lbl = labels.nth(i)
            k = _clean(await lbl.text_content()).rstrip(":")
            val_el = lbl.locator("xpath=following-sibling::*[1]")
            if await val_el.count() and k:
                v = _clean(await val_el.text_content())
                fields.setdefault(_camel(k), v)

        return fields

    async def _extract_address_blocks(self, page: Page) -> dict:
        """
        Extract named address sections: principal office, registered agent,
        service of process, chief officer.

        Returns flat fields matching the output schema.
        """
        detail: dict[str, str] = {}

        # Locate heading elements that introduce address blocks
        headings = page.locator("h2, h3, h4, [class*='section-title' i], [class*='heading' i]")
        count = await headings.count()

        for i in range(count):
            heading = headings.nth(i)
            heading_text = _clean(await heading.text_content()).lower()

            # The block of text that follows the heading
            next_block = heading.locator("xpath=following-sibling::*[1]")
            if not await next_block.count():
                continue
            block_text = _clean(await next_block.text_content())
            lines = [l for l in re.split(r"\n|\r", block_text) if l.strip()]

            if "principal" in heading_text and "office" in heading_text:
                detail.update(self._parse_address_lines(lines, "principalOffice"))
            elif "registered agent" in heading_text:
                detail.update(self._parse_address_lines(lines, "registeredAgent"))
            elif "service of process" in heading_text:
                detail.update(self._parse_address_lines(lines, "serviceOfProcess"))
            elif "chief" in heading_text or "officer" in heading_text:
                detail["chiefOfficerName"] = lines[0] if lines else ""
                if len(lines) > 1:
                    detail.update(self._parse_address_lines(lines[1:], "chiefOfficer"))

        return detail

    @staticmethod
    def _parse_address_lines(lines: list[str], prefix: str) -> dict:
        """
        Best-effort US address line parser.
        Typical format:
            Line 0: Person / Company Name
            Line 1: Street Address
            Line 2: City, State Zip
        """
        out: dict[str, str] = {}
        if not lines:
            return out
        out[f"{prefix}Name"] = _clean(lines[0]) if len(lines) > 1 else ""
        if len(lines) >= 2:
            out[f"{prefix}Addr1"] = _clean(lines[1] if len(lines) > 2 else lines[0])
        if len(lines) >= 3:
            # Parse "CITY, STATE ZIP" or "CITY STATE ZIP"
            csz = _clean(lines[-1])
            m = re.match(
                r"^(.+?)[,\s]+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", csz
            )
            if m:
                out[f"{prefix}City"] = m.group(1).strip()
                out[f"{prefix}State"] = m.group(2)
                out[f"{prefix}Zip"] = m.group(3)
            else:
                out[f"{prefix}CityStateZip"] = csz
        return out

    async def _click_tab_and_scrape_table(
        self, page: Page, tab_name_pattern: re.Pattern
    ) -> list[dict]:
        """
        Click a named tab (if found) and scrape the resulting table.
        Returns a list of row dicts.
        """
        # Try to find and click the tab
        tabs = page.get_by_role("tab")
        tab_count = await tabs.count()
        for i in range(tab_count):
            tab = tabs.nth(i)
            tab_text = _clean(await tab.text_content())
            if tab_name_pattern.search(tab_text):
                try:
                    await tab.click()
                    await asyncio.sleep(0.8)
                except Exception:
                    pass
                break

        return await self._scrape_visible_table(page)

    async def _scrape_visible_table(self, page: Page) -> list[dict]:
        """Return all rows of the first visible table as a list of dicts."""
        rows_data: list[dict] = []

        tables = page.locator("table:visible, table")
        if not await tables.count():
            return rows_data

        table = tables.first
        headers: list[str] = []

        ths = table.locator("th")
        for i in range(await ths.count()):
            headers.append(_camel(_clean(await ths.nth(i).text_content())))

        body_rows = table.locator("tbody tr")
        for r in range(await body_rows.count()):
            row = body_rows.nth(r)
            cells = row.locator("td")
            row_dict: dict[str, str] = {}
            for c in range(await cells.count()):
                key = headers[c] if c < len(headers) else f"col{c}"
                val = _clean(await cells.nth(c).text_content())
                row_dict[key] = val
            if row_dict:
                rows_data.append(row_dict)

        return rows_data

    # ── Client-side filters ───────────────────────────────────────────────────

    def _passes_filters(self, entity: dict) -> bool:
        """Return True if the entity passes all client-side filter criteria."""
        # Date range
        filing = entity.get("dateFiled", "")
        if (self.start_date or self.end_date) and filing:
            if not _in_range(filing, self.start_date, self.end_date):
                return False

        # County
        if self.county_filter:
            if self.county_filter not in entity.get("county", "").upper():
                return False

        # Status
        if self.status_filter:
            if self.status_filter not in entity.get("status", "").upper():
                return False

        return True

    # ── Form helpers ──────────────────────────────────────────────────────────

    @staticmethod
    async def _try_select(
        page: Page,
        selectors: list[str],
        value: str,
        label: str,
    ) -> bool:
        """Try each selector in order until we successfully select an option."""
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count():
                    # Try by value, then by label
                    try:
                        await el.select_option(value=value)
                        return True
                    except Exception:
                        pass
                    try:
                        await el.select_option(label=label)
                        return True
                    except Exception:
                        pass
                    try:
                        await el.select_option(label=re.compile(label, re.I))
                        return True
                    except Exception:
                        pass
            except Exception:
                pass
        return False

    @staticmethod
    async def _try_fill(
        page: Page,
        selectors: list[str],
        value: str,
    ) -> bool:
        """Try each selector in order until we successfully fill an input."""
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count():
                    await el.fill(value)
                    return True
            except Exception:
                pass
        return False

    # ── API response normaliser ───────────────────────────────────────────────

    @staticmethod
    def _normalise_api_record(raw: dict) -> dict:
        """
        Map whatever field names the backend uses to our standard output schema.
        Works with multiple candidate field naming conventions.
        """
        def pick(*keys: str) -> str:
            for k in keys:
                if k in raw:
                    return str(raw[k]).strip()
            return ""

        return {
            "dosId":            pick("dosId", "dos_id", "id", "entityId"),
            "entityName":       pick("entityName", "name", "entity_name", "legalName"),
            "entityType":       pick("entityType", "entity_type", "type"),
            "status":           pick("status", "entityStatus"),
            "county":           pick("county", "principalCounty"),
            "jurisdiction":     pick("jurisdiction", "state", "homeState"),
            "fictName":         pick("fictName", "assumedName", "fictitiousName"),
            "dateFiled":        _parse_date(pick("dateFiled", "date_filed", "filingDate", "dateOfFirstFiling")) or pick("dateFiled", "date_filed", "filingDate"),
            "effectiveDate":    _parse_date(pick("effectiveDate", "effective_date")) or pick("effectiveDate"),
            "documentType":     pick("documentType", "document_type", "lastDocument"),
        }
