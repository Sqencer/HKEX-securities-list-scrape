from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, re, json, os, time, random, threading
import pandas as pd
# ──────────────────────────────────────────────────────────────────────────────
class EXCELHANDLER:

    FILE = 'ListOfSecurities.xlsx'
    URL  = ('https://www.hkex.com.hk/eng/services/trading/securities/'
            'securitieslists/ListOfSecurities.xlsx')
    PAGE = ('https://www.hkex.com.hk/Services/Trading/Securities/'
            'Securities-Lists?sc_lang=en')
    HEADERS = {
        'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/124.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection':      'keep-alive',
    }

    def download(self):
        session  = requests.Session()

        # Visit securities list page first to get session cookies
        session.get(self.PAGE, headers=self.HEADERS, timeout=30)

        response = session.get(
            self.URL,
            headers={
                **self.HEADERS,
                'Referer': self.PAGE,
                'Accept':  ('application/vnd.openxmlformats-officedocument'
                            '.spreadsheetml.sheet,*/*'),
            },
            timeout=30
        )
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        if 'html' in content_type:
            raise ValueError(
                f"Got HTML instead of xlsx.\n"
                f"Response preview: {response.text[:300]}"
            )

        with open(self.FILE, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded '{self.FILE}' ({len(response.content)/1024:.1f} KB)")

    def load(self, force_download: bool = False) -> pd.DataFrame:
        if force_download or not os.path.exists(self.FILE):
            self.download()
        else:
            print(f"Using cached '{self.FILE}'")
        return self.read_dataframe(self.FILE)

    def read_dataframe(self, file) -> pd.DataFrame:
        df = pd.read_excel(file, skiprows=2, engine='openpyxl')
        print(f"Loaded DataFrame: {df.shape}")
        return df

    def save_excel(self, df: pd.DataFrame, filename: str = 'output.xlsx'):
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"Saved '{filename}'")


# ──────────────────────────────────────────────────────────────────────────────
class HKEXScraper:

    KEYS = {
        'Sec_name_en'  : 'issuer_name',
        'Industry_1'   : 'hsic_ind_classification',
        'Industry_2'   : 'hsic_sub_sector_classification',
        'List_date'    : 'listing_date',
        'TOT_issue_shs': 'amt_os',
        'Issue_shs_A'  : 'issued_shares_class_A',
        'Issue_shs_B'  : 'issued_shares_class_B',
        'Issue_date'   : 'shares_issued_date',
        'POI'          : 'incorpin',
        'Price'        : 'hi',
        'List_Cat'     : 'listing_category',
        'Registrar'    : 'registrar',
        'EPS'          : 'eps',
        'P/E'          : 'pe',
        'Mkt_Cap'      : 'mkt_cap',
    }

    def __init__(self):
        self.token            = None
        self.token_fetched_at = 0
        self.TOKEN_TTL        = 25 * 60
        self.session          = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/124.0.0.0 Safari/537.36',
            'Referer':    'https://www.hkex.com.hk/',
        })


    def _refresh_token(self):
        res = self.session.get(
            'https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/'
            'Equities-Quote?sym=700&sc_lang=en',
            timeout=30,
        )

        match = re.search(
            r'LabCI\.getToken\s*=\s*function\s*\(\s*\)\s*\{[^}]*return\s*"([^"]+)"',
            res.text
        )
        if match:
            self.token            = match.group(1)
            self.token_fetched_at = time.time()
        else:
            raise ValueError("Token not found — page structure may have changed")

    def get_token(self) -> str:
        with _token_refresh_lock:  # only one thread refreshes at a time
            if not self.token or (time.time() - self.token_fetched_at > self.TOKEN_TTL):
                self._refresh_token()
        return self.token

    def fetch_quote(self, sym: str, _retry: bool = True) -> dict:
        token     = self.get_token()
        clean_sym = str(sym).lstrip("0") or "0"
        qid       = int(time.time() * 1000)
        url       = (
            f'https://www1.hkex.com.hk/hkexwidget/data/getequityquote'
            f'?sym={clean_sym}&token={token}&lang=eng&qid={qid}&callback=NULL'
        )

        raw = self.session.get(url, timeout=30).text.strip()

        if not raw.startswith("NULL("):
            raise ValueError(f"Unexpected response for {sym}: {raw[:200]}")

        try:
            data = json.loads(raw[5:-1])
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parse failed for {sym}: {e}")

        if data.get('data', {}).get('responseCode') == 'F':
            if not _retry:
                raise ValueError(f"API failure for {sym} after token refresh")
            self._refresh_token()
            return self.fetch_quote(sym, _retry=False)

        return data['data']['quote']

    def clean_data(self, sym: str, data: dict) -> dict:
        result = {'stock_code': str(sym).zfill(5)}
        for col_name, api_key in self.KEYS.items():
            result[col_name] = data.get(api_key, '')
        return result


# ──────────────────────────────────────────────────────────────────────────────
_thread_local = threading.local()
_token_refresh_lock = threading.Lock()
def get_thread_scraper() -> HKEXScraper:
    if not hasattr(_thread_local, 'scraper'):
        _thread_local.scraper = HKEXScraper()
    return _thread_local.scraper


def scrape_single(code: str) -> dict:
    scraper = get_thread_scraper()
    time.sleep(random.uniform(0.5, 0.1))
    for attempt in range(3):
        try:
            quote = scraper.fetch_quote(code)
            return scraper.clean_data(code, quote)
        except Exception as e:
            if attempt < 2:
                time.sleep((2 ** attempt) + random.uniform(0, 1))
            else:
                empty = {'stock_code': str(code).zfill(5)}
                for col in HKEXScraper.KEYS:
                    empty[col] = ''
                empty['error'] = str(e)
                print(e)
                return empty


# ──────────────────────────────────────────────────────────────────────────────
def run_with_retry(
    workers:    int  = 5,
    max_passes: int  = 3,
    batch_size: int  = 200,
):
    
    start = time.perf_counter()
    handler       = EXCELHANDLER()

    original_df   = handler.load()
    original_df = handler.read_dataframe(handler.FILE)
    first_col     = original_df.columns[0]
    original_df[first_col] = original_df[first_col].astype(str).str.zfill(5)
    all_codes     = original_df[first_col].tolist()
    print(f"Total unique stock codes: {len(all_codes)}")

    results   = {}
    pending   = all_codes
    pass_num  = 0

    while pending and pass_num < max_passes:
        pass_num += 1
        errors    = []
        completed = 0
        print(f"\n── Pass {pass_num}/{max_passes} | "
              f"Stocks to scrape: {len(pending)} ──")

        try:
            for batch_start in range(0, len(pending), batch_size):
                batch = pending[batch_start : batch_start + batch_size]

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {
                        executor.submit(scrape_single, code): code
                        for code in batch
                    }
                    for future in as_completed(futures):
                        try:
                            row  = future.result()
                            code = row['stock_code']
                            results[code] = row
                            if row.get('error'):
                                errors.append(code)
                        except Exception as e:
                            code = futures[future]
                            errors.append(code)
                            print(f"[{code}] Unhandled: {e}")

                        completed += 1
                        if completed % 100 == 0:
                            print(f"Progress: {completed}/{len(pending)} | "
                                  f"Errors: {len(errors)}")
                            elapsed = time.perf_counter() - start
                            print(f"Time: {elapsed:.2f}s | "
                            f"Rate: {completed/elapsed:.1f} stocks/sec")

                if batch_start + batch_size < len(pending):
                    time.sleep(random.uniform(3, 6))

        except KeyboardInterrupt:
            print(f"\nKeyboardInterrupted. Saving {len(results)} partial results...")
            break

        print(f"\nPass {pass_num} done — "
              f"Success: {len(pending) - len(errors)} | "
              f"Errors: {len(errors)}")

        pending = errors

        if pending and pass_num < max_passes:
            wait = 10 * pass_num
            print(f"Waiting {wait}s before pass {pass_num + 1}...")
            time.sleep(wait)

    final_errors = [c for c, r in results.items() if r.get('error')]
    if final_errors:
        print(f"\n{len(final_errors)} permanently failed after {max_passes} passes.")

    scraped_df = pd.DataFrame(results.values())
    joined_df  = original_df.merge(
        scraped_df, left_on=first_col, right_on='stock_code', how='left'
    )
    joined_df.drop(columns=['stock_code'], inplace=True, errors='ignore')
    handler.save_excel(joined_df, 'output.xlsx')
    elapsed = time.perf_counter() - start
    print(f"Time: {elapsed:.2f}s | "
        f"Rate: {len(all_codes)/elapsed:.1f} stocks/sec")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    run_with_retry(workers=10, max_passes=3)
