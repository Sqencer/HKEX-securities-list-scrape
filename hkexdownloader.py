
try:
    import pip_system_certs.wrapt_requests
    pip_system_certs.wrapt_requests.inject_truststore()
except Exception as e:
    print(f"[WARN] Could not enable system certificate store: {e}")

import os, re, json, time
import requests
from datetime import date, datetime

from tqdm import tqdm

# ─── CONFIGURATION ────────────────────────────────────────────────────────────

STOCK_CODES = "stock.txt"

OUTPUT_DIR       = "./hkex_downloads"
GO_DIR           = os.path.join(OUTPUT_DIR, "Global_Offering")
AR_DIR           = os.path.join(OUTPUT_DIR, "Allotment_Results")

DATE_FROM        = "19990101"
DATE_TO          = date.today().strftime("%Y%m%d")
REQUEST_DELAY    = 1.5

BASE_URL    = "https://www1.hkexnews.hk"
SEARCH_URL  = f"{BASE_URL}/search/titlesearch.xhtml"
SERVLET_URL = f"{BASE_URL}/search/titleSearchServlet.do"
STOCKS_JSON = f"{BASE_URL}/ncms/script/eds/activestock_sehk_e.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": SEARCH_URL,
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def load_stock_codes_from_txt(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read()

    # Replace newlines with commas, then split by comma
    raw_numbers = content.replace("\n", ",").split(",")

    # Strip spaces and add leading zeroes to make each number 5 digits
    numbers = [
        number.strip().zfill(5)
        for number in raw_numbers
        if number.strip()
    ]

    return numbers

def sanitize(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name).strip()[:100]

def parse_file_size(file_info: str) -> float:
    s = (file_info or "").strip().upper()
    m = re.match(r"([\d.]+)\s*(KB|MB|GB|B)?", s)
    if not m:
        return 0.0
    val  = float(m.group(1))
    unit = m.group(2) or "B"
    return val * {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}[unit]

def is_pdf(rec: dict) -> bool:
    ftype = (rec.get("FILE_TYPE") or "").upper()
    flink = (rec.get("FILE_LINK") or "").lower()
    return ftype == "PDF" or flink.endswith(".pdf")

def is_global_offering(title: str) -> bool:
    return title.strip().upper() == "GLOBAL OFFERING"

def is_allotment_result(title: str) -> bool:
    return "ALLOTMENT RESULT" in title.strip().upper()

def pick_largest(recs: list) -> dict | None:
    pdf_recs = [r for r in recs if is_pdf(r)]
    if not pdf_recs:
        return None
    return max(pdf_recs, key=lambda r: parse_file_size(r.get("FILE_INFO", "")))

# ─── SESSION + STOCK MAP ──────────────────────────────────────────────────────

def init_session():
    session = requests.Session()
    session.headers.update(HEADERS)


    resp = session.get(SEARCH_URL, timeout=30)
    resp.raise_for_status()
    vs_m     = re.search(r'name="javax\\.faces\\.ViewState"[^>]*value="([^"]+)"', resp.text)
    action_m = re.search(r'<form[^>]+action="([^"]+)"', resp.text)
    viewstate  = vs_m.group(1) if vs_m else ""
    action_url = action_m.group(1) if action_m else "/search/titlesearch.xhtml"
    if action_url.startswith("/"):
        action_url = BASE_URL + action_url
    return session, viewstate, action_url

def load_stock_map(session):
    resp = session.get(STOCKS_JSON, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return {s["c"]: int(s["i"]) for s in resp.json()}

# ─── CORE SEARCH ──────────────────────────────────────────────────────────────

def servlet_search(session, viewstate, action_url, stock_id,
                   search_type, t1code, title_kw="", doc_type=""):
    payload = {
        "j_idt10":                               "j_idt10",
        "j_idt10:loadMoreRange":                 "1000",
        "javax.faces.ViewState":                 viewstate,
        "titleSearchResultControl.searchByIndex":"0",
        "titleSearchByAllResult.dateFromUi":     "",
        "titleSearchByAllResult.dateToUi":       "",
        "stockId":      str(stock_id),
        "market":       "SEHK",
        "category":     "0",
        "searchType":   search_type,
        "documentType": doc_type,
        "t1code":       t1code,
        "t2code":       "",
        "t2Gcode":      "",
        "from":         DATE_FROM,
        "to":           DATE_TO,
    }
    session.post(action_url, data=payload,
                 headers={**HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
                 timeout=60)
    time.sleep(REQUEST_DELAY)

    params = {
        "sortDir": "0", "sortByOptions": "DateTime",
        "category": "0", "market": "SEHK",
        "stockId":      str(stock_id),
        "documentType": doc_type,
        "fromDate":     DATE_FROM,
        "toDate":       DATE_TO,
        "title":        title_kw,
        "searchType":   search_type,
        "t1code":       t1code,
        "t2Gcode": "", "t2code": "",
        "rowRange": "1000", "lang": "EN",
    }
    resp = session.get(SERVLET_URL, params=params,
                       headers={**HEADERS, "Accept": "application/json",
                                "X-Requested-With": "XMLHttpRequest"},
                       timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("recordCnt", 0) == 0:
        return []
    return json.loads(data.get("result", "[]"))

# ─── DOWNLOAD ─────────────────────────────────────────────────────────────────

def download_pdf(url, filepath, session):
    try:
        r = session.get(url, timeout=60, stream=True)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=16384):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"    ✗ Download error: {e}")
        return False

def save_doc(rec, folder, padded, session):
    """
    Download one document into the given folder.
    Filename: {stock_code}_{date}.pdf
    Returns status: "downloaded" | "exists" | "failed"
    """
    file_link = rec.get("FILE_LINK", "")
    if not file_link:
        return "failed"
    if not file_link.startswith("http"):
        file_link = BASE_URL + file_link

    date_str = rec.get("DATE_TIME", "")[:10].replace("/", "-")
    filename = f"{padded}_{date_str}.pdf"
    filepath = os.path.join(folder, filename)

    if os.path.exists(filepath):
        print(f"  ✓ Already exists: {filename}")
        return "exists"

    size_label = rec.get("FILE_INFO", "?")
    print(f"  ↓ [{padded}] {os.path.basename(folder)} ({size_label})")
    ok = download_pdf(file_link, filepath, session)
    time.sleep(REQUEST_DELAY)
    return "downloaded" if ok else "failed"

# ─── PROCESS ONE STOCK ────────────────────────────────────────────────────────

def process_stock(stock_code, stock_map, session, viewstate, action_url):
    padded   = str(stock_code).zfill(5)
    stock_id = stock_map.get(padded)
    result   = {"stock": padded, "global_offering": "not_found", "allotment": "not_found"}

    if not stock_id:
        print(f"  ✗ [{padded}] Not in active stock list")
        return result

    # Single broad search — fetch all docs, filter locally
    all_docs = servlet_search(
        session, viewstate, action_url,
        stock_id=stock_id,
        search_type="rbAfter2006",
        t1code="",
        title_kw="",
    )
    print(f"  [{padded}] {len(all_docs)} docs found")

    # ── Global Offering: largest PDF titled exactly "GLOBAL OFFERING" ─────────
    go_candidates = [r for r in all_docs if is_global_offering(r.get("TITLE", ""))]
    go_rec = pick_largest(go_candidates)
    if go_rec:
        result["global_offering"] = save_doc(go_rec, GO_DIR, padded, session)
    else:
        print(f"  ⚠ [{padded}] No Global Offering PDF found")

    # ── Allotment Results: earliest PDF with "ALLOTMENT RESULT" in title ──────
    ar_candidates = [r for r in all_docs
                     if is_allotment_result(r.get("TITLE", "")) and is_pdf(r)]
    if ar_candidates:
        ar_rec = min(ar_candidates, key=lambda r: datetime.strptime(r.get("DATE_TIME", ""), "%d/%m/%Y %H:%M"))
        result["allotment"] = save_doc(ar_rec, AR_DIR, padded, session)
    else:
        print(f"  ⚠ [{padded}] No Allotment Results found")

    return result

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    # Create both output folders upfront
    os.makedirs(GO_DIR, exist_ok=True)
    os.makedirs(AR_DIR, exist_ok=True)
    stock_codes = load_stock_codes_from_txt(STOCK_CODES)

    
    if not stock_codes:
        print(f"No stock codes found in {STOCK_CODES}")
        return


    print(f"Loaded {len(stock_codes)} stock codes from {STOCK_CODES}")
    print(f"Stock codes: {stock_codes}")


    print("Initializing session...")
    session, viewstate, action_url = init_session()
    print("Loading stock map...")
    stock_map = load_stock_map(session)
    print(f"  {len(stock_map)} active stocks loaded")
    print(f"  Date range : {DATE_FROM} → {DATE_TO}")
    print(f"  Global Offering  → {GO_DIR}")
    print(f"  Allotment Results→ {AR_DIR}\n")

    all_results = []

    for i, code in enumerate(tqdm(stock_codes, desc="Stocks")):
        padded = str(code).zfill(5)
        result = process_stock(padded, stock_map, session, viewstate, action_url)
        all_results.append(result)

        # Refresh ViewState every 15 stocks
        if (i + 1) % 15 == 0:
            print("\n[Refreshing session...]")
            session, viewstate, action_url = init_session()
            time.sleep(REQUEST_DELAY)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print(f"  {'Stock':<8} {'Global Offering':<25} {'Allotment Results'}")
    print("  " + "-" * 60)
    for r in all_results:
        go_sym = "✓" if r["global_offering"] in ("downloaded","exists") else "✗"
        ar_sym = "✓" if r["allotment"]       in ("downloaded","exists") else "✗"
        print(f"  {r['stock']:<8} {go_sym} {r['global_offering']:<23} {ar_sym} {r['allotment']}")

    total   = len(all_results)
    go_ok   = sum(1 for r in all_results if r["global_offering"] in ("downloaded","exists"))
    ar_ok   = sum(1 for r in all_results if r["allotment"]       in ("downloaded","exists"))
    both_ok = sum(1 for r in all_results if r["global_offering"] in ("downloaded","exists")
                                         and r["allotment"]      in ("downloaded","exists"))
    print("=" * 65)
    print(f"  Global Offering found   : {go_ok}/{total}")
    print(f"  Allotment Results found : {ar_ok}/{total}")
    print(f"  Both docs found         : {both_ok}/{total}")
    print(f"  Saved to                : {os.path.abspath(OUTPUT_DIR)}/")
    print("=" * 65)

    missing = [r["stock"] for r in all_results
               if r["global_offering"] == "not_found" or r["allotment"] == "not_found"]
    if missing:
        print(f"\n  Stocks with missing docs ({len(missing)}): {missing}")
    

if __name__ == "__main__":
    main()
    input("Press Enter to exit...")