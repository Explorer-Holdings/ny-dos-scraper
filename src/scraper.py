"""
NY DOS Business Entity Scraper — NY Open Data (SODA API) backend.

Replaces the Playwright/browser approach entirely.

NY Open Data SODA datasets used (no auth required, no proxy needed):
  63wc-4exh  – All Filings  (one row per filing, many rows per entity)
               Fields: DOS ID Number, Filing Num, Date Filed, Eff Date,
                       Entity Type, Document Type, Entity Name, Fict Name,
                       Cnty Prin Ofc, Jurisdiction
  7jkw-gj56  – Active Corporations (one row per active entity, richer fields)
               Fields: DOS ID, Entity Name, Status, Entity Type, Date of
                       Initial DOS Filing, County, Jurisdiction, CEO Name,
                       Registered Agent, Service of Process Address,
                       Principal Executive Office Address, DOS Process Address

Strategy
--------
1. Primary search uses 7jkw-gj56 (active entities, one row per entity, richer).
2. If the user also wants inactive/historical data, we additionally query 63wc-4exh
   and deduplicate on DOS ID.
3. All filtering (name, entity type, date, county, status) is pushed to the
   server-side SODA $where clause for efficiency.
4. Pagination is handled automatically — SODA max page size is 50,000.
"""

from __future__ import annotations

import asyncio
import re
import string
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

import httpx

# ── SODA API constants ────────────────────────────────────────────────────────

SODA_BASE = "https://data.ny.gov/resource"

# Dataset 1: Active & historical entities (rich fields, one row per entity)
DS_ACTIVE = f"{SODA_BASE}/7jkw-gj56.json"

# Dataset 2: All filings (one row per filing — good for date-range / recent)
DS_FILINGS = f"{SODA_BASE}/63wc-4exh.json"

# SODA page size
PAGE_SIZE = 50_000

# Request headers — polite bot identification
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "NY-DOS-Biz-Scraper/2.0 (Apify Actor; contact via Apify)",
}

# Field name mappings: DS_ACTIVE column names → our output schema
ACTIVE_FIELD_MAP = {
    "dos_id_"                          : "dosId",
    "current_entity_name"              : "entityName",
    "initial_dos_filing_date"          : "dateFiled",
    "county"                           : "county",
    "jurisdiction"                     : "jurisdiction",
    "entity_type"                      : "entityType",
    "dos_process_name"                 : "dosProcessName",
    "dos_process_address_1"            : "dosProcessAddr1",
    "dos_process_address_2"            : "dosProcessAddr2",
    "dos_process_city"                 : "dosProcessCity",
    "dos_process_state"                : "dosProcessState",
    "dos_process_zip_code"             : "dosProcessZip",
    "ceo_name"                         : "chiefOfficerName",
    "ceo_address_1"                    : "chiefOfficerAddr1",
    "ceo_address_2"                    : "chiefOfficerAddr2",
    "ceo_city"                         : "chiefOfficerCity",
    "ceo_state"                        : "chiefOfficerState",
    "ceo_zip_code"                     : "chiefOfficerZip",
    "registered_agent_name"            : "registeredAgentName",
    "registered_agent_address_1"       : "registeredAgentAddr1",
    "registered_agent_address_2"       : "registeredAgentAddr2",
    "registered_agent_city"            : "registeredAgentCity",
    "registered_agent_state"           : "registeredAgentState",
    "registered_agent_zip_code"        : "registeredAgentZip",
    "principal_executive_office_address_1" : "principalOfficeAddr1",
    "principal_executive_office_address_2" : "principalOfficeAddr2",
    "principal_executive_office_city"  : "principalOfficeCity",
    "principal_executive_office_state" : "principalOfficeState",
    "principal_executive_office_zip_code": "principalOfficeZip",
}

# Field name mappings: DS_FILINGS column names → our output schema
FILINGS_FIELD_MAP = {
    "dos_id_number"  : "dosId",
    "date_filed"     : "dateFiled",
    "eff_date"       : "effectiveDate",
    "entity_type"    : "entityType",
    "document_type"  : "documentType",
    "entity_name"    : "entityName",
    "fict_name"      : "fictName",
    "cnty_prin_ofc"  : "county",
    "jurisdiction"   : "jurisdiction",
}

ENTITY_DETAIL_URL = "https://apps.dos.ny.gov/publicInquiry/#DOS-{dos_id}"


def _clean(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v)).strip()


def _parse_date(raw: str) -> str:
    """Return ISO date string YYYY-MM-DD from various formats."""
    if not raw:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip()[:26], fmt).date().isoformat()
        except ValueError:
            pass
    return raw.strip()[:10]


def _soda_escape(value: str) -> str:
    """Escape single quotes for SODA $where clauses."""
    return value.replace("'", "''")


class NYDOSScraper:
    """Scraper using NY Open Data SODA API — no browser, no proxy required."""

    def __init__(
        self,
        input_data: dict[str, Any],
        push_callback,
        logger,
    ) -> None:
        self.name_search: str = input_data.get("nameSearch", "").strip()
        self.search_by: str = input_data.get("searchBy", "entityName")
        self.search_type: str = input_data.get("searchType", "BEGINS_WITH")
        self.active_only: bool = input_data.get("nameType", False)
        self.entity_type: str = input_data.get("entityType", "").strip()
        self.start_date: str | None = input_data.get("startDate") or None
        self.end_date: str | None = input_data.get("endDate") or None
        self.county_filter: str = input_data.get("county", "").strip().upper()
        self.status_filter: str = input_data.get("statusFilter", "").strip().upper()
        self.max_results: int = int(input_data.get("maxResults", 500))

        self._push = push_callback
        self.log = logger
        self._count = 0

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self) -> int:
        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=60.0,
            follow_redirects=True,
        ) as client:
            self._client = client
            await self._scrape()
        return self._count

    # ── Orchestration ─────────────────────────────────────────────────────────

    async def _scrape(self) -> None:
        """
        Choose the right dataset and query strategy based on inputs.

        - DOS ID lookup → DS_FILINGS (group by dos_id_number, exact match)
        - Name search with active_only OR no date filter → DS_ACTIVE (richer)
        - Date-range / recent filings → DS_FILINGS (has exact date_filed)
        - Blank name + date filter → DS_FILINGS sorted by date_filed DESC
        """
        if self.search_by == "dosId" and self.name_search:
            self.log.info(f"Looking up DOS ID: {self.name_search}")
            await self._query_filings(dos_id=self.name_search)

        elif self.start_date or self.end_date:
            # Date-range mode — use filings dataset, sorted newest first
            self.log.info(
                f"Date-range search: {self.start_date or 'any'} → {self.end_date or 'any'}"
            )
            await self._query_filings()

        else:
            # Name search or broad sweep — use active entities dataset
            self.log.info(
                f"Entity search: name={self.name_search!r} type={self.search_type} "
                f"entity_type={self.entity_type!r} active_only={self.active_only}"
            )
            await self._query_active()

    # ── DS_ACTIVE queries ─────────────────────────────────────────────────────

    async def _query_active(self) -> None:
        """Query the 7jkw-gj56 (active entities) dataset."""
        where_clauses = self._build_active_where()
        where_str = " AND ".join(where_clauses) if where_clauses else "1=1"

        self.log.info(f"SODA $where: {where_str}")

        offset = 0
        seen: set[str] = set()

        while self._count < self.max_results:
            params = {
                "$where": where_str,
                "$limit": min(PAGE_SIZE, self.max_results - self._count + 1000),
                "$offset": offset,
                "$order": "current_entity_name ASC",
            }
            rows = await self._fetch(DS_ACTIVE, params)
            if not rows:
                break

            for row in rows:
                if self._count >= self.max_results:
                    break
                dos_id = _clean(row.get("dos_id_", row.get("dos_id", "")))
                if dos_id in seen:
                    continue
                seen.add(dos_id)
                record = self._map_active_record(row, dos_id)
                if self._passes_filters(record):
                    await self._emit(record)

            if len(rows) < PAGE_SIZE:
                break
            offset += len(rows)
            await asyncio.sleep(0.3)

    def _build_active_where(self) -> list[str]:
        clauses = []

        # Name search
        if self.name_search:
            escaped = _soda_escape(self.name_search.upper())
            if self.search_by == "dosId":
                clauses.append(f"dos_id_='{escaped}'")
            else:
                if self.search_type == "BEGINS_WITH":
                    clauses.append(f"upper(current_entity_name) LIKE '{escaped}%'")
                elif self.search_type == "CONTAINS":
                    clauses.append(f"upper(current_entity_name) LIKE '%{escaped}%'")
                else:  # SOUNDS_LIKE — SODA doesn't support it, fall back to contains
                    clauses.append(f"upper(current_entity_name) LIKE '%{escaped}%'")

        # Entity type
        if self.entity_type:
            escaped_et = _soda_escape(self.entity_type.upper())
            clauses.append(f"upper(entity_type)='{escaped_et}'")

        # County
        if self.county_filter:
            clauses.append(f"upper(county)='{_soda_escape(self.county_filter)}'")

        return clauses

    def _map_active_record(self, row: dict, dos_id: str) -> dict:
        record: dict[str, Any] = {"dosId": dos_id}
        for api_key, out_key in ACTIVE_FIELD_MAP.items():
            val = row.get(api_key, "")
            record[out_key] = _clean(val)
        # Normalise date
        record["dateFiled"] = _parse_date(record.get("dateFiled", ""))
        # Status — this dataset only has active entities, default Active
        record.setdefault("status", "Active")
        record["url"] = ENTITY_DETAIL_URL.format(dos_id=dos_id)
        record["scrapedAt"] = datetime.utcnow().isoformat() + "Z"
        return record

    # ── DS_FILINGS queries ────────────────────────────────────────────────────

    async def _query_filings(self, dos_id: str | None = None) -> None:
        """Query the 63wc-4exh (all filings) dataset."""
        where_clauses = self._build_filings_where(dos_id=dos_id)
        where_str = " AND ".join(where_clauses) if where_clauses else "1=1"

        self.log.info(f"Filings SODA $where: {where_str}")

        offset = 0
        seen: set[str] = set()

        while self._count < self.max_results:
            params = {
                "$where": where_str,
                "$limit": min(PAGE_SIZE, self.max_results - self._count + 5000),
                "$offset": offset,
                "$order": "date_filed DESC",
            }
            rows = await self._fetch(DS_FILINGS, params)
            if not rows:
                break

            for row in rows:
                if self._count >= self.max_results:
                    break
                entity_dos_id = _clean(row.get("dos_id_number", ""))
                # One record per entity — take most recent filing (already sorted DESC)
                if entity_dos_id in seen:
                    continue
                seen.add(entity_dos_id)
                record = self._map_filings_record(row, entity_dos_id)
                if self._passes_filters(record):
                    await self._emit(record)

            if len(rows) < PAGE_SIZE:
                break
            offset += len(rows)
            await asyncio.sleep(0.3)

    def _build_filings_where(self, dos_id: str | None = None) -> list[str]:
        clauses = []

        if dos_id:
            clauses.append(f"dos_id_number='{_soda_escape(dos_id)}'")
            return clauses

        # Name search
        if self.name_search:
            escaped = _soda_escape(self.name_search.upper())
            if self.search_type == "BEGINS_WITH":
                clauses.append(f"upper(entity_name) LIKE '{escaped}%'")
            elif self.search_type == "CONTAINS":
                clauses.append(f"upper(entity_name) LIKE '%{escaped}%'")
            else:
                clauses.append(f"upper(entity_name) LIKE '%{escaped}%'")

        # Entity type
        if self.entity_type:
            clauses.append(f"upper(entity_type)='{_soda_escape(self.entity_type.upper())}'")

        # Date range
        if self.start_date:
            clauses.append(f"date_filed >= '{self.start_date}T00:00:00.000'")
        if self.end_date:
            clauses.append(f"date_filed <= '{self.end_date}T23:59:59.000'")

        # County
        if self.county_filter:
            clauses.append(f"upper(cnty_prin_ofc)='{_soda_escape(self.county_filter)}'")

        return clauses

    def _map_filings_record(self, row: dict, dos_id: str) -> dict:
        record: dict[str, Any] = {"dosId": dos_id}
        for api_key, out_key in FILINGS_FIELD_MAP.items():
            record[out_key] = _clean(row.get(api_key, ""))
        record["dateFiled"] = _parse_date(record.get("dateFiled", ""))
        record["effectiveDate"] = _parse_date(record.get("effectiveDate", ""))
        record.setdefault("status", "")
        record["url"] = ENTITY_DETAIL_URL.format(dos_id=dos_id)
        record["scrapedAt"] = datetime.utcnow().isoformat() + "Z"
        return record

    # ── Filters ───────────────────────────────────────────────────────────────

    def _passes_filters(self, record: dict) -> bool:
        # Status filter (client-side — DS_ACTIVE only has active, DS_FILINGS has no status)
        if self.status_filter:
            status = record.get("status", "").upper()
            if status and self.status_filter not in status:
                return False
        return True

    # ── HTTP ──────────────────────────────────────────────────────────────────

    async def _fetch(self, url: str, params: dict) -> list[dict]:
        """Fetch one page from the SODA API with retry."""
        for attempt in range(3):
            try:
                resp = await self._client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                self.log.info(f"  API returned {len(data)} rows (offset={params.get('$offset', 0)})")
                return data
            except httpx.HTTPStatusError as exc:
                self.log.error(f"HTTP {exc.response.status_code} from SODA API: {exc}")
                if exc.response.status_code == 429:
                    await asyncio.sleep(5 * (attempt + 1))
                else:
                    break
            except Exception as exc:
                self.log.warning(f"Fetch attempt {attempt + 1} failed: {exc}")
                await asyncio.sleep(2 * (attempt + 1))
        return []

    # ── Output ────────────────────────────────────────────────────────────────

    async def _emit(self, record: dict) -> None:
        await self._push(record)
        self._count += 1
        if self._count % 100 == 0:
            self.log.info(f"  ✔ {self._count} records saved…")
