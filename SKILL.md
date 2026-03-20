---
name: sirene-to-notion
description: Extract French company data from the INSEE SIRENE API and push it into a Notion database. Use this skill whenever the user wants to find, list, or extract French companies by department, city, employee count (tranche d'effectifs), or NAF/APE code and populate a Notion database with the results. Also triggers when the user mentions SIRENE, INSEE, annuaire des entreprises, base SIRENE, recherche d'entreprises françaises, or wants to build a prospect list of French companies. Works for any combination of geographic and size filters.
---

# SIRENE to Notion — Company Data Extraction & Import

This skill extracts French company data from the public SIRENE database (via the API Recherche d'Entreprises) and imports it into a Notion database.

## Overview

The workflow has two phases:
1. **Extract** — Query the API Recherche d'Entreprises (free, no API key needed) to get company data filtered by department, employee count, NAF code, etc.
2. **Load** — Push the extracted data into a Notion database using the Notion MCP or API.

## Prerequisites

- Internet access to `recherche-entreprises.api.gouv.fr` (free, no auth required)
- Notion MCP connected, or Notion API token available
- `curl` and `python3` available (standard on most systems)

## Step 1: Understand the User's Filters

Ask the user for their criteria. Common filters include:

| Filter | API Parameter | Examples |
|--------|--------------|---------|
| Department | `departement` | `06` (Alpes-Maritimes), `75` (Paris), `13` (Bouches-du-Rhône) |
| Employee range | `tranche_effectif_salarie` | `12` (20-49), `21` (50-99), `22` (100-199) |
| NAF/APE code | `activite_principale` | `62.01Z` (programming), `47.11F` (supermarkets) |
| Legal status | `nature_juridique` | `5710` (SAS), `5499` (SARL) |
| City (commune code) | `code_commune` | Use INSEE commune codes |
| Active only | `etat_administratif` | `A` (active, default) |

### SIRENE Employee Tranche Codes Reference

These are the official INSEE codes — users will typically describe ranges in plain language and you need to map them:

| Code | Range |
|------|-------|
| `00` | 0 employees |
| `01` | 1-2 |
| `02` | 3-5 |
| `03` | 6-9 |
| `11` | 10-19 |
| `12` | 20-49 |
| `21` | 50-99 |
| `22` | 100-199 |
| `31` | 200-249 |
| `32` | 250-499 |
| `41` | 500-999 |
| `42` | 1000-1999 |
| `51` | 2000-4999 |
| `52` | 5000-9999 |
| `53` | 10000+ |

When the user says "20 to 100 employees", map to codes `12,21`.
When the user says "PME", map to codes `12,21,22,31`.

## Step 2: Run the Extraction Script

Use the Python script at `scripts/extract_sirene.py` to extract data:

```bash
python3 /path/to/skill/scripts/extract_sirene.py \
  --departement 06 \
  --tranches 12,21 \
  --output /tmp/sirene_results.json
```

The script handles pagination automatically (the API returns max 25 results per page, and caps at 10,000 total results). It includes rate limiting to respect the 7 calls/second limit.

### Script Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--departement` | Yes | INSEE department code (e.g., `06`, `75`) |
| `--tranches` | No | Comma-separated tranche codes (e.g., `12,21`) |
| `--naf` | No | NAF/APE activity code filter |
| `--nature-juridique` | No | Legal form filter |
| `--commune` | No | INSEE commune code |
| `--output` | Yes | Output JSON file path |
| `--active-only` | No | Only active companies (default: true) |

### Output Format

The script outputs a JSON array where each entry contains:

```json
{
  "siren": "123456789",
  "nom": "COMPANY NAME",
  "siret_siege": "12345678900001",
  "adresse": "123 RUE EXAMPLE, 06000 NICE",
  "code_naf": "62.01Z",
  "libelle_naf": "Programmation informatique",
  "tranche_effectif": "12",
  "tranche_label": "20 à 49 salariés",
  "nombre_etablissements": 3,
  "date_creation": "2015-03-20",
  "nature_juridique": "SAS"
}
```

### Handling Large Datasets

The API caps results at 10,000 entries. If the query returns more:
- The script will warn about truncation
- Suggest splitting by NAF section or commune to get complete coverage
- The script supports a `--section-naf` parameter to filter by top-level NAF section (A through U)

For very large extractions (e.g., all companies in a big department), run the script multiple times with different NAF sections and merge results.

## Step 3: Push to Notion

Use the script `scripts/push_to_notion.py` to import into Notion:

```bash
python3 /path/to/skill/scripts/push_to_notion.py \
  --input /tmp/sirene_results.json \
  --notion-database-id "32850ff7a0738091ad43ded808e915c7" \
  --mapping /path/to/skill/scripts/default_mapping.json
```

### Column Mapping

The push script uses a JSON mapping file to connect SIRENE fields to Notion columns. The default mapping (`scripts/default_mapping.json`) handles common column names, but you should check the user's actual Notion database schema first using the Notion fetch tool.

If the user's Notion schema doesn't match the default mapping, create a custom mapping file. The format is:

```json
{
  "notion_column_name": "sirene_field_name",
  "Name": "nom",
  "Address": "adresse",
  "Employees": "tranche_effectif_midpoint",
  "Code NAF": "code_naf",
  "ID": "siren",
  "Status": "_default:Not started"
}
```

Special mapping values:
- `"_default:VALUE"` — Sets a fixed default value
- `"_empty"` — Leaves the column empty
- `"tranche_effectif_midpoint"` — Converts tranche code to a numeric midpoint (e.g., code 12 → 35)

### Rate Limiting for Notion

The Notion API has rate limits. The push script:
- Batches creates in groups of 10 (Notion's max per request for the MCP)
- Waits 350ms between batches
- Retries on 429 errors with exponential backoff
- Logs progress every 50 entries

### Alternative: Direct Notion MCP Push

If running inside Claude Code with Notion MCP connected, you can skip the push script and use the Notion MCP `create-pages` tool directly. Create pages in batches of up to 100:

```
Use Notion:notion-create-pages with parent data_source_id from the database.
Map fields according to the database schema.
Set Status to "Not started" for all entries.
```

## Step 4: Enrichment (Optional)

The SIRENE database doesn't include websites, LinkedIn URLs, or revenue data. For enrichment:

1. **Pappers.fr API** (paid) — Provides CA (chiffre d'affaires), website, dirigeants
2. **Societe.com scraping** — Public data but rate-limited
3. **Google search** — For website discovery: `"{company_name}" "{city}" site:linkedin.com/company`
4. **Clearbit/Apollo/Clay** — Commercial enrichment tools

The enrichment step is separate and should be proposed to the user as a follow-up.

## Error Handling

Common issues:
- **API returns 0 results**: Check tranche codes (they're not sequential — e.g., 12 is 20-49, NOT 12 employees). Double-check department code.
- **Truncated at 10,000**: Split query by NAF section or commune
- **Notion rate limit**: The push script handles this automatically with backoff
- **Duplicate entries**: The script deduplicates by SIREN before pushing

## Example Workflows

**"List all companies in Nice with 50-99 employees"**
```bash
python3 scripts/extract_sirene.py --departement 06 --commune 06088 --tranches 21 --output /tmp/nice_50_99.json
python3 scripts/push_to_notion.py --input /tmp/nice_50_99.json --notion-database-id <DB_ID>
```

**"All PMEs in Alpes-Maritimes"**
```bash
python3 scripts/extract_sirene.py --departement 06 --tranches 12,21,22,31 --output /tmp/pme_06.json
```

**"Tech companies (NAF 62) in Paris with 20+ employees"**
```bash
python3 scripts/extract_sirene.py --departement 75 --tranches 12,21,22,31,32,41,42,51,52,53 --naf 62 --output /tmp/tech_paris.json
```
