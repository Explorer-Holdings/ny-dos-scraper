# 🗽 NY DOS Business Entity Scraper

Scrape New York State business entities from the official
[NY Department of State Public Inquiry portal](https://apps.dos.ny.gov/publicInquiry/).

Built for deployment as an **Apify Actor** — upload to the Apify console,
set your inputs, hit **Start**, and get back clean JSON, CSV or Excel.

---

## ✨ What It Extracts

| Field | Description |
|---|---|
| `dosId` | NY DOS Corporation ID |
| `entityName` | Registered legal name |
| `entityType` | Domestic Corp, LLC, LP, LLP, NFP, etc. |
| `status` | Active, Inactive, Dissolved, Revoked … |
| `county` | NY county of principal office |
| `jurisdiction` | Home state / country for foreign entities |
| `fictName` | Assumed / fictitious name (if any) |
| `dateFiled` | Date of first filing (ISO `YYYY-MM-DD`) |
| `effectiveDate` | Effective date of most recent document |
| `documentType` | Most recent filing document type |
| `principalOfficeName` | Name on principal office address |
| `principalOfficeAddr1` | Street line |
| `principalOfficeCity/State/Zip` | |
| `registeredAgentName` | Registered agent name |
| `registeredAgentAddr1` | Street line |
| `registeredAgentCity/State/Zip` | |
| `chiefOfficerName` | CEO / chief officer name |
| `filingHistory` | Array of all filings with dates and document types |
| `displayNameHistory` | Display name changes over time |
| `mergerHistory` | Merger / consolidation records |
| `assumedNameHistory` | DBA / assumed name records |
| `url` | Direct link to the entity's NY DOS page |
| `scrapedAt` | ISO timestamp of when data was collected |

---

## 🔧 Input Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `searchBy` | select | `entityName` | Field to search by: Entity Name, DOS ID, Assumed Name, Assumed Name ID |
| `nameSearch` | string | *(blank)* | Name, ID, or keyword to search |
| `nameType` | boolean | `false` | Active-Only names only (`true`) or all names (`false`) |
| `searchType` | select | `BEGINS_WITH` | Name matching: **Begins With**, **Contains**, or **Sounds Like** (phonetic) |
| `entityType` | select | *(all)* | Filter by entity type (LLC, Corp, LP, etc.) |
| `startDate` | date | *(none)* | Only include entities filed **on or after** this date |
| `endDate` | date | *(none)* | Only include entities filed **on or before** this date |
| `county` | string | *(none)* | NY county filter, e.g. `NEW YORK`, `KINGS`, `NASSAU` |
| `statusFilter` | select | *(all)* | Status filter: Active, Dissolved, Revoked, etc. |
| `scrapeDetails` | boolean | `true` | Visit each entity page for full details (slower but complete) |
| `maxResults` | integer | `500` | Maximum records to return |
| `proxyConfiguration` | proxy | Residential | Apify proxy — **RESIDENTIAL group required** (WAF blocks datacenter IPs) |

---

## 📦 Example Inputs

### Search by company name

```json
{
  "searchBy": "entityName",
  "nameSearch": "APPLE",
  "searchType": "BEGINS_WITH",
  "entityType": "DOMESTIC BUSINESS CORP",
  "maxResults": 50
}
```

### Direct DOS ID lookup

```json
{
  "searchBy": "dosId",
  "nameSearch": "3494",
  "scrapeDetails": true,
  "maxResults": 1
}
```

### Most recently registered LLCs in New York County

```json
{
  "entityType": "DOMESTIC LIMITED LIABILITY COMPANY",
  "county": "NEW YORK",
  "startDate": "2025-01-01",
  "nameSearch": "",
  "maxResults": 500
}
```

### All active foreign corporations (alphabetical sweep — slow)

```json
{
  "entityType": "FOREIGN BUSINESS CORP",
  "statusFilter": "ACTIVE",
  "nameSearch": "",
  "maxResults": 10000
}
```

---

## 📊 Example Output Record

```json
{
  "dosId": "3494",
  "entityName": "APPLETON & COX, INC.",
  "entityType": "FOREIGN BUSINESS CORPORATION",
  "status": "Inactive",
  "county": "New York",
  "jurisdiction": "DE",
  "fictName": "",
  "dateFiled": "2019-01-28",
  "effectiveDate": "2019-01-28",
  "documentType": "CERTIFICATE OF CHANGE (BY AGENT)",
  "principalOfficeName": "C T CORPORATION SYSTEM",
  "principalOfficeAddr1": "28 LIBERTY ST.",
  "principalOfficeCity": "NEW YORK",
  "principalOfficeState": "NY",
  "principalOfficeZip": "10005",
  "registeredAgentName": "C T CORPORATION SYSTEM",
  "registeredAgentAddr1": "28 LIBERTY ST.",
  "registeredAgentCity": "NEW YORK",
  "registeredAgentState": "NY",
  "registeredAgentZip": "10005",
  "chiefOfficerName": "",
  "filingHistory": [
    { "documentType": "CERTIFICATE OF CHANGE (BY AGENT)", "dateFiled": "2019-01-28", "effectiveDate": "2019-01-28" }
  ],
  "displayNameHistory": [],
  "mergerHistory": [],
  "assumedNameHistory": [],
  "url": "https://apps.dos.ny.gov/publicInquiry/#DOS-3494",
  "scrapedAt": "2026-04-18T10:30:00.000Z"
}
```

---

## 🏗️ Architecture

```
main.py
└── NYDOSScraper.run()
    ├── Apify proxy setup (RESIDENTIAL group)
    ├── Playwright Chromium launch
    └── _scrape_with_terms()
        ├── For each search term:
        │   ├── New browser context (fresh cookies)
        │   ├── Intercept XHR/fetch to discover backend API
        │   ├── Fill & submit search form
        │   ├── Parse results table (DOM scraping)
        │   └── For each entity:
        │       ├── Client-side filters (date, county, status)
        │       ├── Navigate to detail page (#DOS-XXXX)
        │       ├── Extract KV pairs, addresses, tab tables
        │       └── Push to Apify dataset
        └── Alphabetical sweep (A–Z) if no nameSearch provided
```

**Why Playwright?**
The NY DOS site is a Vue.js SPA protected by a WAF that returns `403 Host not in allowlist` for all direct API calls from datacenter IPs. Playwright with residential proxies is the only reliable approach.

**API Discovery**
The scraper intercepts JSON responses during form submission to auto-discover the SPA's backend REST endpoints. If found, these endpoints are reused for faster subsequent calls without full page loads.

---

## 🚀 Deploying to Apify

```bash
# Install Apify CLI
npm install -g apify-cli

# Log in
apify login

# From the project directory
apify push
```

Then open the Apify Console, find your actor, and run it with the desired inputs.

---

## ⚠️ Important Notes

- **Residential proxy required.** The NY DOS WAF blocks datacenter IPs. Enable `RESIDENTIAL` proxy group in the proxy configuration input.
- **Site result limit.** The NY DOS portal caps results at 500 per search query. For larger datasets, the actor automatically runs multiple searches.
- **Rate limiting.** The actor inserts a ~1.2 s delay between page navigations to avoid hammering the server.
- **`scrapeDetails: false`** is much faster (no per-entity page visits) but only returns the 7 summary-table columns.
- **Alphabetical sweep** (blank `nameSearch`) can take a long time — use `entityType` and/or date filters to narrow scope.

---

## ⚖️ Legal

This actor accesses publicly available records from the NY Department of State's public database, the same data accessible to anyone via the official website. It is not affiliated with, endorsed by, or sponsored by the New York State Department of State.
