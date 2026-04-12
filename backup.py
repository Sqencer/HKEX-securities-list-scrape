from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, re, json, os, csv, time, threading
import pandas as pd
from io import BytesIO


class EXCELHANDLER:
    def __init__(self):
        self.url = 'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://www.hkex.com.hk/',
        }       

    def download(self):
        response = requests.get(self.url, headers= self.headers, timeout = 30)
        response.raise_for_status()
        with open('ListOfSecurities.xlsx', 'wb') as f:
          f.write(response.content)

    def read_dataframe(self):
        df = pd.read_excel("ListOfSecurities.xlsx", skiprows=2, engine="openpyxl")
        return df
    
    def save_excel(self, df: pd.DataFrame, filename: str = 'output.xlsx'):
        # NEW: was missing entirely from your class, run() called it and would crash
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"Saved '{filename}'")
    


class HKEXScraper:
    KEYS = {
        "Sec_name_en" : "issuer_name",
        "Industry_1" : "hsic_ind_classification",
        "Industry_2" : "hsic_sub_sector_classification",
        "List_date" : "listing_date",
        "TOT_issue_shs" : "amt_os",
        "Issue_shs_A" : "issued_shares_class_A",
        "Issue_shs_B" : "issued_shares_class_B",
        "Issue_date" : "shares_issued_date",
        "POI" : "incorpin",
        "Price" : "hi",
        "List_Cat" : "listing_category",
        "Registrar" : "registrar",
        "EPS" : "eps",
        "P/E" : "pe",
        "Mkt_Cap" : "mkt_cap"
    }
    def __init__(self):
        self.token = None
        self.token_fetched_at = 0
        self.TOKEN_TTL = 25 * 60  # refresh every 25 mins (before 30-min expiry)
        self.session = requests.Session()
        self._token_lock = threading.Lock()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://www.hkex.com.hk/',
        })

    def _refresh_token(self):
        res = self.session.get(
            'https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym=700&sc_lang=en', timeout=30
        )
        match = re.search(
            r'LabCI\.getToken\s*=\s*function\s*\(\s*\)\s*\{[^}]*return\s*"([^"]+)"',
            res.text
        )
        if match:
            self.token = match.group(1)
            self.token_fetched_at = time.time()
        else:
            raise ValueError("Token not found")

    def get_token(self) -> str:
        # Auto-refresh if expired or not yet fetched
        with self._token_lock:
            if not self.token or (time.time() - self.token_fetched_at > self.TOKEN_TTL):
                self._refresh_token()
        return self.token

    def fetch_quote(self, sym: str, _retry: bool = True) -> dict:
        token = self.get_token()
        sym = sym.lstrip("0")
        qid = int(time.time() * 1000)
        url = (f'https://www1.hkex.com.hk/hkexwidget/data/getequityquote?sym={sym}&token={token}&lang=eng&qid={qid}&callback=NULL')
        raw = self.session.get(url, timeout=40).text.strip()


        try:
            data = json.loads(raw[5:-1])    # strip NULL(...)
        except json.JSONDecodeError:
            raise ValueError(f"Unexpected response for {sym}: {raw[:100]}")

        # If token expired mid-session, retry once with a fresh token
        if data.get('data', {}).get('responseCode') == 'F':
            if not _retry:
                raise ValueError(f"API returned failure for {sym} after token refresh")
            self._refresh_token()
            return self.fetch_quote(sym, _retry = False)

        return data["data"]["quote"]
    
    def clean_data(self, sym: str, data: dict) -> dict:
        result = {'stock_code': sym.zfill(5)}
        for col_name, api_key in self.KEYS.items():
            result[col_name] = data.get(api_key, '')
        return result



def scrape_single(scraper: HKEXScraper, code: str) -> dict:
    try:
        time.sleep(0.1)
        quote = scraper.fetch_quote(code)
        return scraper.clean_data(code, quote)
    except Exception as e:
        # Return an error row so the stock code still appears in the output
        result = {'stock_code': str(code).zfill(5)}
        for col_name in HKEXScraper.KEYS:
            result[col_name] = ''
        result['error'] = str(e)
        print(e)
        return result


def run(workers: int = 5):
    start = time.perf_counter()
    handler = EXCELHANDLER()
    shared_scraper = HKEXScraper()

    # Step 1: Load original DataFrame
    #handler.download()
    original_df = handler.read_dataframe()
    first_col = original_df.columns[0]
    original_df[first_col] = original_df[first_col].astype(str).str.zfill(5)
    stock_codes = original_df.iloc[:,0].tolist()
    print("Data read")

    # Step 3: Scrape each stock code concurrently
    results = []
    completed = 0
    pending = stock_codes
    max_pass = 3
    while pending and max_pass>0:
        error = []
        max_pass -= 1

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(scrape_single, shared_scraper, code): code for code in pending}
            for future in as_completed(futures):
                try:
                    single_result = future.result()
                    if single_result.get("error"):
                        error.append(code)
                    else:
                        results.append(single_result)
                except Exception as e:
                    code = futures[future]
                    error.append(code)
                    print(f"[{code}] Unhandled error: {e}")
                completed += 1
                if completed % 100 == 0:
                    print(f"Progress: {completed}/{len(stock_codes)} | "
                        f"Error: {len(error)}")
                    elapsed = time.perf_counter() - start
                    print(f"Time: {elapsed:.2f}s | "
                        f"Rate: {completed/elapsed:.1f} stocks/sec")
                    
        pending = error
    
    final_errors = [c for c, r in results.items() if r.get('error')]
    if final_errors:
        print(f"\n{len(final_errors)} permanently failed after {max_pass} passes.")



    # Step 4: Build scraped DataFrame
    scraped_df = pd.DataFrame(results)
    print(f"Scraped DataFrame: {scraped_df.shape}")

    # Step 5: Join the two DataFrames on stock code
    joined_df = original_df.merge(
        scraped_df,
        left_on=first_col,
        right_on='stock_code',
        how='left'       # keep all original rows even if scrape failed
    )
    print(f"Joined DataFrame: {joined_df.shape}")

    # Step 6: Output to Excel
    handler.save_excel(joined_df, 'output.xlsx')
    elapsed = time.perf_counter() - start
    print(f"Total Time: {elapsed:.2f}s | "
        f"Rate: {len(stock_codes)/elapsed:.1f} stocks/sec")



if __name__ == '__main__':
    run(4)

