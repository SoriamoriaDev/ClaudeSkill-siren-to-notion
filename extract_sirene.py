#!/usr/bin/env python3
"""
SIRENE Company Extractor
Queries the API Recherche d'Entreprises (recherche-entreprises.api.gouv.fr)
to extract French company data filtered by department, employee count, etc.

This API is free and requires no authentication.
Rate limit: 7 requests/second.
Max results: 10,000 per query.
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.parse
import urllib.error

API_BASE = "https://recherche-entreprises.api.gouv.fr/search"
MAX_PER_PAGE = 25
MAX_TOTAL = 10000
RATE_LIMIT_DELAY = 0.15  # ~7 requests/sec

TRANCHE_LABELS = {
    "00": "0 salarié",
    "01": "1 à 2 salariés",
    "02": "3 à 5 salariés",
    "03": "6 à 9 salariés",
    "11": "10 à 19 salariés",
    "12": "20 à 49 salariés",
    "21": "50 à 99 salariés",
    "22": "100 à 199 salariés",
    "31": "200 à 249 salariés",
    "32": "250 à 499 salariés",
    "41": "500 à 999 salariés",
    "42": "1 000 à 1 999 salariés",
    "51": "2 000 à 4 999 salariés",
    "52": "5 000 à 9 999 salariés",
    "53": "10 000 salariés et plus",
}

TRANCHE_MIDPOINTS = {
    "00": 0, "01": 1, "02": 4, "03": 7,
    "11": 15, "12": 35, "21": 75, "22": 150,
    "31": 225, "32": 375, "41": 750, "42": 1500,
    "51": 3500, "52": 7500, "53": 15000,
}

# NAF top-level sections for splitting large queries
NAF_SECTIONS = [
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
    "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U"
]


def build_url(params: dict, page: int) -> str:
    """Build the API URL with query parameters."""
    query_params = {"per_page": str(MAX_PER_PAGE), "page": str(page)}

    if params.get("departement"):
        query_params["departement"] = params["departement"]
    if params.get("tranches"):
        query_params["tranche_effectif_salarie"] = params["tranches"]
    if params.get("naf"):
        query_params["activite_principale"] = params["naf"]
    if params.get("section_naf"):
        query_params["section_activite_principale"] = params["section_naf"]
    if params.get("nature_juridique"):
        query_params["nature_juridique"] = params["nature_juridique"]
    if params.get("commune"):
        query_params["code_commune"] = params["commune"]
    if params.get("active_only", True):
        query_params["etat_administratif"] = "A"

    return f"{API_BASE}?{urllib.parse.urlencode(query_params)}"


def format_address(siege: dict) -> str:
    """Format the siege address into a single string."""
    parts = []
    if siege.get("numero_voie"):
        parts.append(siege["numero_voie"])
    if siege.get("type_voie"):
        parts.append(siege["type_voie"])
    if siege.get("libelle_voie"):
        parts.append(siege["libelle_voie"])
    
    street = " ".join(parts)
    
    code_postal = siege.get("code_postal", "")
    commune = siege.get("libelle_commune", "")
    
    if street and (code_postal or commune):
        return f"{street}, {code_postal} {commune}".strip()
    elif code_postal or commune:
        return f"{code_postal} {commune}".strip()
    elif street:
        return street
    return ""


def parse_company(result: dict) -> dict:
    """Parse a single API result into our standard format."""
    siege = result.get("siege", {})
    tranche = result.get("tranche_effectif_salarie", "") or ""
    
    return {
        "siren": result.get("siren", ""),
        "nom": result.get("nom_complet", "") or result.get("nom_raison_sociale", ""),
        "siret_siege": siege.get("siret", ""),
        "adresse": format_address(siege),
        "code_naf": result.get("activite_principale", ""),
        "libelle_naf": result.get("libelle_activite_principale", ""),
        "tranche_effectif": tranche,
        "tranche_label": TRANCHE_LABELS.get(tranche, "Non renseigné"),
        "tranche_effectif_midpoint": TRANCHE_MIDPOINTS.get(tranche, 0),
        "nombre_etablissements": result.get("nombre_etablissements_ouverts", 0),
        "date_creation": result.get("date_creation", ""),
        "nature_juridique": result.get("nature_juridique", ""),
    }


def fetch_page(url: str) -> dict:
    """Fetch a single page from the API."""
    req = urllib.request.Request(url, headers={"User-Agent": "SIRENE-Extractor/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print("  ⚠ Rate limited, waiting 2 seconds...", file=sys.stderr)
            time.sleep(2)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        raise


def extract(params: dict) -> list[dict]:
    """Extract all companies matching the given parameters."""
    companies = []
    seen_sirens = set()
    page = 1
    total = None

    while True:
        url = build_url(params, page)
        
        if page == 1:
            print(f"🔍 Querying: {url}", file=sys.stderr)
        
        try:
            data = fetch_page(url)
        except Exception as e:
            print(f"  ❌ Error on page {page}: {e}", file=sys.stderr)
            break

        if total is None:
            total = data.get("total_results", 0)
            print(f"  📊 Total results: {total}", file=sys.stderr)
            if total > MAX_TOTAL:
                print(f"  ⚠ Results capped at {MAX_TOTAL}. Consider splitting by NAF section.", file=sys.stderr)

        results = data.get("results", [])
        if not results:
            break

        for r in results:
            company = parse_company(r)
            if company["siren"] and company["siren"] not in seen_sirens:
                seen_sirens.add(company["siren"])
                companies.append(company)

        page_count = (total + MAX_PER_PAGE - 1) // MAX_PER_PAGE if total else 1
        if page % 10 == 0 or page == page_count:
            print(f"  📄 Page {page}/{min(page_count, MAX_TOTAL // MAX_PER_PAGE)} — {len(companies)} companies collected", file=sys.stderr)

        if len(results) < MAX_PER_PAGE:
            break
        if page * MAX_PER_PAGE >= min(total or 0, MAX_TOTAL):
            break

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    return companies


def main():
    parser = argparse.ArgumentParser(description="Extract companies from SIRENE API")
    parser.add_argument("--departement", required=True, help="INSEE department code (e.g., 06)")
    parser.add_argument("--tranches", help="Comma-separated tranche codes (e.g., 12,21)")
    parser.add_argument("--naf", help="NAF/APE activity code filter (e.g., 62.01Z)")
    parser.add_argument("--section-naf", help="NAF section letter (A-U) for broad filtering")
    parser.add_argument("--nature-juridique", help="Legal form code")
    parser.add_argument("--commune", help="INSEE commune code")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    parser.add_argument("--active-only", action="store_true", default=True, help="Only active companies (default: true)")
    parser.add_argument("--split-by-naf-section", action="store_true", help="Auto-split by NAF section to bypass 10k limit")

    args = parser.parse_args()

    params = {
        "departement": args.departement,
        "tranches": args.tranches,
        "naf": args.naf,
        "section_naf": args.section_naf,
        "nature_juridique": args.nature_juridique,
        "commune": args.commune,
        "active_only": args.active_only,
    }

    if args.split_by_naf_section:
        print(f"🔄 Splitting extraction by NAF section to bypass 10k limit...", file=sys.stderr)
        all_companies = []
        seen_sirens = set()
        for section in NAF_SECTIONS:
            section_params = {**params, "section_naf": section}
            print(f"\n📂 Section NAF {section}:", file=sys.stderr)
            results = extract(section_params)
            for c in results:
                if c["siren"] not in seen_sirens:
                    seen_sirens.add(c["siren"])
                    all_companies.append(c)
            print(f"  ✅ {len(results)} companies in section {section} ({len(all_companies)} total unique)", file=sys.stderr)
        companies = all_companies
    else:
        companies = extract(params)

    # Write output
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Done! {len(companies)} companies saved to {args.output}", file=sys.stderr)

    # Print summary stats
    tranche_counts = {}
    for c in companies:
        label = c.get("tranche_label", "Unknown")
        tranche_counts[label] = tranche_counts.get(label, 0) + 1
    
    print("\n📊 Breakdown by employee range:", file=sys.stderr)
    for label, count in sorted(tranche_counts.items()):
        print(f"   {label}: {count}", file=sys.stderr)


if __name__ == "__main__":
    main()
