#Recommend running in late night to avoid traffic
#Runs for at least 30min

from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, re, json, time, threading
import pandas as pd


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
        self.TOKEN_TTL = 25 * 60
        self.session = requests.Session()
        self._token_lock = threading.Lock()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.hkex.com.hk/',
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'script',
            'sec-fetch-mode': 'no-cors',
            'sec-fetch-site': 'same-site',
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
        with self._token_lock:
            if not self.token or (time.time() - self.token_fetched_at > self.TOKEN_TTL):
                self._refresh_token()
        return self.token

    def fetch_quote(self, sym: str, _retry: bool = True) -> dict:
        token = self.get_token()
        sym = sym.lstrip("0")
        qid = int(time.time() * 1000)
        callback = f"jQuery35_{qid}" 
        url = (f'https://www1.hkex.com.hk/hkexwidget/data/getequityquote?sym={sym}&token={token}&lang=eng&qid={qid}&callback={callback}&_={qid+1}')
        raw = self.session.get(url, timeout=30).text.strip()


        try:
            data = json.loads(raw[len(callback)+1:-1])
        except json.JSONDecodeError:
            raise ValueError(f"Unexpected response for {sym}: {raw[:100]}")

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
        result = {'stock_code': str(code).zfill(5)}
        for col_name in HKEXScraper.KEYS:
            result[col_name] = ''
        result['error'] = str(e)
        print(e)
        return result


def run(workers: int = 3):
    start = time.perf_counter()
    handler = EXCELHANDLER()
    shared_scraper = HKEXScraper()


    #handler.download()
    original_df = handler.read_dataframe()
    first_col = original_df.columns[0]
    original_df[first_col] = original_df[first_col].astype(str).str.zfill(5)
    stock_codes = original_df.iloc[:,0].tolist()
    print("Data read")


    results = []
    pending = stock_codes
    max_pass = 3
    while pending and max_pass>0:
        completed = 0
        error = []
        print(f"Running Remaining:{max_pass} | Pending: {len(pending)}")
        max_pass -= 1

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(scrape_single, shared_scraper, code): code for code in pending}
            for future in as_completed(futures):
                try:
                    single_result = future.result()
                    if single_result.get("error") and max_pass != 0:
                        error.append(code)
                    else:
                        results.append(single_result)
                except Exception as e:
                    code = futures[future]
                    error.append(code)
                    print(f"[{code}] Unhandled error: {e}")
                completed += 1
                if completed % 100 == 0 or completed == len(pending):
                    print(f"Round: {3-max_pass} | Progress: {completed}/{len(pending)} | "
                        f"Error: {len(error)}")
                    elapsed = time.perf_counter() - start
                    print(f"Time: {elapsed:.2f}s | "
                        f"Rate: {completed/elapsed:.1f} stocks/sec")
                    
        pending = error
    
    if pending:
        print(f"\n{len(pending)} permanently failed after {max_pass} passes.")
    else: 
        print("All error cleared")




    scraped_df = pd.DataFrame(results)
    print(f"Scraped DataFrame: {scraped_df.shape}")


    joined_df = original_df.merge(
        scraped_df,
        left_on=first_col,
        right_on='stock_code',
        how='left'
    )
    print(f"Joined DataFrame: {joined_df.shape}")


    handler.save_excel(joined_df, 'output.xlsx')
    elapsed = time.perf_counter() - start
    print(f"Total Time: {elapsed:.2f}s | "
        f"Rate: {len(stock_codes)/elapsed:.1f} stocks/sec")



if __name__ == '__main__':
    run(5)

