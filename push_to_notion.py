#!/usr/bin/env python3
"""
Push SIRENE extraction results to a Notion database.

Supports two modes:
1. Notion API (requires NOTION_API_TOKEN env var)
2. JSON output for manual import or Notion MCP usage

Rate limits: Notion API allows ~3 requests/second.
The script batches and throttles accordingly.
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error

NOTION_API_VERSION = "2022-06-28"
NOTION_API_BASE = "https://api.notion.com/v1"
BATCH_SIZE = 1  # Notion API creates one page per request (unless using MCP)
RATE_LIMIT_DELAY = 0.35  # ~3 requests/sec
MAX_RETRIES = 3

TRANCHE_MIDPOINTS = {
    "00": 0, "01": 1, "02": 4, "03": 7,
    "11": 15, "12": 35, "21": 75, "22": 150,
    "31": 225, "32": 375, "41": 750, "42": 1500,
    "51": 3500, "52": 7500, "53": 15000,
}


def load_mapping(mapping_path: str) -> dict:
    """Load column mapping from JSON file."""
    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_companies(input_path: str) -> list[dict]:
    """Load extracted companies from JSON."""
    with open(input_path, "r", encoding="utf-8") as f:
        return json.load(f)


def map_company_to_notion(company: dict, mapping: dict, schema: dict) -> dict:
    """Map a SIRENE company record to Notion page properties."""
    properties = {}

    for notion_col, sirene_field in mapping.items():
        if sirene_field.startswith("_default:"):
            value = sirene_field[len("_default:"):]
        elif sirene_field == "_empty":
            continue
        elif sirene_field == "tranche_effectif_midpoint":
            value = TRANCHE_MIDPOINTS.get(company.get("tranche_effectif", ""), 0)
        else:
            value = company.get(sirene_field, "")

        # Determine Notion property type from schema
        col_type = schema.get(notion_col, {}).get("type", "text")

        if col_type == "title":
            properties[notion_col] = {
                "title": [{"text": {"content": str(value)[:2000]}}]
            }
        elif col_type == "number":
            try:
                properties[notion_col] = {"number": float(value) if value else None}
            except (ValueError, TypeError):
                properties[notion_col] = {"number": None}
        elif col_type == "url":
            url_val = str(value) if value else None
            if url_val and not url_val.startswith("http"):
                url_val = f"https://{url_val}" if url_val else None
            properties[notion_col] = {"url": url_val}
        elif col_type == "status":
            properties[notion_col] = {"status": {"name": str(value)}}
        elif col_type == "select":
            properties[notion_col] = {"select": {"name": str(value)}}
        elif col_type == "rich_text" or col_type == "text":
            properties[notion_col] = {
                "rich_text": [{"text": {"content": str(value)[:2000]}}]
            }
        else:
            # Default to rich_text
            properties[notion_col] = {
                "rich_text": [{"text": {"content": str(value)[:2000]}}]
            }

    return properties


def create_notion_page(database_id: str, properties: dict, token: str) -> dict:
    """Create a single page in Notion via the API."""
    url = f"{NOTION_API_BASE}/pages"
    payload = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_API_VERSION,
        },
        method="POST",
    )

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  ⚠ Rate limited, waiting {wait}s (attempt {attempt + 1}/{MAX_RETRIES})...", file=sys.stderr)
                time.sleep(wait)
            else:
                body = e.read().decode("utf-8") if hasattr(e, "read") else ""
                print(f"  ❌ HTTP {e.code}: {body[:200]}", file=sys.stderr)
                raise
    raise Exception("Max retries exceeded")


def generate_mcp_batch(companies: list[dict], mapping: dict, data_source_id: str) -> list[dict]:
    """Generate a batch format suitable for Notion MCP create-pages tool.
    
    This outputs the simplified property format that the Notion MCP expects
    (flat key-value pairs, not the full Notion API property structure).
    """
    pages = []
    for i, company in enumerate(companies):
        props = {}
        for notion_col, sirene_field in mapping.items():
            if sirene_field.startswith("_default:"):
                value = sirene_field[len("_default:"):]
            elif sirene_field == "_empty":
                continue
            elif sirene_field == "tranche_effectif_midpoint":
                value = TRANCHE_MIDPOINTS.get(company.get("tranche_effectif", ""), 0)
            else:
                value = company.get(sirene_field, "")
            
            # For MCP, use the userDefined: prefix for special property names
            col_key = notion_col
            if notion_col.lower() in ("id", "url"):
                col_key = f"userDefined:{notion_col}"
            
            props[col_key] = value if value else None

        pages.append({"properties": props})

    return pages


def main():
    parser = argparse.ArgumentParser(description="Push SIRENE data to Notion")
    parser.add_argument("--input", required=True, help="Input JSON file from extract_sirene.py")
    parser.add_argument("--notion-database-id", required=True, help="Notion database ID")
    parser.add_argument("--mapping", required=True, help="Column mapping JSON file")
    parser.add_argument("--mode", choices=["api", "mcp-json"], default="api",
                       help="'api' = direct Notion API calls, 'mcp-json' = output JSON for MCP tool")
    parser.add_argument("--schema", help="Notion schema JSON (required for 'api' mode)")
    parser.add_argument("--data-source-id", help="Notion data source ID (for 'mcp-json' mode)")
    parser.add_argument("--mcp-output", help="Output file for MCP batch JSON")
    parser.add_argument("--start-index", type=int, default=0, help="Start from this index (for resuming)")
    parser.add_argument("--limit", type=int, help="Max number of companies to push")

    args = parser.parse_args()

    companies = load_companies(args.input)
    mapping = load_mapping(args.mapping)

    if args.start_index:
        companies = companies[args.start_index:]
    if args.limit:
        companies = companies[:args.limit]

    print(f"📥 Loaded {len(companies)} companies from {args.input}", file=sys.stderr)
    print(f"📋 Column mapping: {json.dumps(mapping, indent=2)}", file=sys.stderr)

    if args.mode == "mcp-json":
        # Generate JSON suitable for Notion MCP create-pages tool
        data_source_id = args.data_source_id or args.notion_database_id
        
        # Split into batches of 100 (MCP limit)
        batch_size = 100
        all_batches = []
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            pages = generate_mcp_batch(batch, mapping, data_source_id)
            all_batches.append({
                "batch_index": i // batch_size,
                "start": i,
                "end": min(i + batch_size, len(companies)),
                "data_source_id": data_source_id,
                "pages": pages,
            })

        output = {
            "total_companies": len(companies),
            "total_batches": len(all_batches),
            "batches": all_batches,
        }

        out_path = args.mcp_output or "/tmp/notion_mcp_batches.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Generated {len(all_batches)} MCP batches → {out_path}", file=sys.stderr)
        print(f"   Use Notion MCP create-pages tool with data_source_id: {data_source_id}", file=sys.stderr)

    elif args.mode == "api":
        token = os.environ.get("NOTION_API_TOKEN")
        if not token:
            print("❌ NOTION_API_TOKEN environment variable required for API mode", file=sys.stderr)
            print("   Set it with: export NOTION_API_TOKEN=ntn_...", file=sys.stderr)
            sys.exit(1)

        if not args.schema:
            print("❌ --schema required for API mode (JSON file describing Notion column types)", file=sys.stderr)
            sys.exit(1)

        with open(args.schema, "r", encoding="utf-8") as f:
            schema = json.load(f)

        success = 0
        errors = 0

        for i, company in enumerate(companies):
            try:
                properties = map_company_to_notion(company, mapping, schema)
                create_notion_page(args.notion_database_id, properties, token)
                success += 1
            except Exception as e:
                errors += 1
                print(f"  ❌ Error on company {company.get('nom', 'unknown')}: {e}", file=sys.stderr)

            if (i + 1) % 50 == 0:
                print(f"  📊 Progress: {i + 1}/{len(companies)} ({success} ok, {errors} errors)", file=sys.stderr)

            time.sleep(RATE_LIMIT_DELAY)

        print(f"\n✅ Done! {success} created, {errors} errors out of {len(companies)} companies", file=sys.stderr)


if __name__ == "__main__":
    main()
