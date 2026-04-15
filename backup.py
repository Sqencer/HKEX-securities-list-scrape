#Recommend running in late night to avoid traffic
#Runs for at least 30min

from concurrent.futures import ThreadPoolExecutor, as_completed
import requests, re, json, time, threading, zhconv
import pandas as pd


class EXCELHANDLER:
    def __init__(self):
        self.url_en = 'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx'
        self.url_tc = 'https://www.hkex.com.hk/chi/services/trading/securities/securitieslists/ListOfSecurities_c.xlsx'

        self.headers = {
            'User-Agent': 'Mozilla/5.0 Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://www.hkex.com.hk/',
        }       

    def downloaden(self):
        response = requests.get(self.url_en, headers= self.headers, timeout = 30)
        response.raise_for_status()
        with open('ListOfSecurities_en.xlsx', 'wb') as f:
          f.write(response.content)


    def downloadtc_read(self):
        response = requests.get(self.url_tc, headers= self.headers, timeout = 30)
        response.raise_for_status()
        with open('ListOfSecurities_tc.xlsx', 'wb') as f:
          f.write(response.content)

        df = pd.read_excel("ListOfSecurities_tc.xlsx", skiprows=2, engine="openpyxl")
        first_two_col = df.iloc[:, 0:2]
        first_two_col['股份名称'] = first_two_col['股份名稱'].apply(self.convert_tcsc)
        first_two_col['股份代號'] = first_two_col['股份代號'].astype(str).str.zfill(5)
        return first_two_col

    def read_dataframe(self):
        df = pd.read_excel("ListOfSecurities_en.xlsx", skiprows=2, engine="openpyxl")
        return df
    
    def save_excel(self, df: pd.DataFrame, filename: str = 'output.xlsx'):
        df.to_excel(filename, index=False, engine='openpyxl')
        print(f"Saved '{filename}'")

    def convert_tcsc(self, text):
        if isinstance(text, str):
            return zhconv.convert(text, "zh-hans")
        return text

    


class HKEXScraper:
    KEYS = {
        "Sec_name_en" : "nm",
        "Issuere_name": "issuer_name",
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
        "Mkt_Cap(B)" : "mkt_cap"
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
        return result




def run(workers: int = 3):
    start = time.perf_counter()
    handler = EXCELHANDLER()
    shared_scraper = HKEXScraper()


    handler.downloaden()
    chi_col = handler.downloadtc_read()
    original_df = handler.read_dataframe()
    first_col = original_df.columns[0]
    original_df[first_col] = original_df[first_col].astype(str).str.zfill(5)
    stock_codes = original_df.iloc[:,0].tolist()
    original_df = pd.concat([chi_col, original_df], axis=1)
    print("Data read")


    results = []
    pending = stock_codes
    max_pass = 3
    while pending and max_pass>0:
        round_time = time.perf_counter()
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
                        failcode = futures[future]
                        error.append(failcode)
                        print(f"{failcode} got an error")
                    elif single_result.get("error") and max_pass == 0:
                        results.append(single_result)
                        failcode = futures[future]
                        error.append(failcode)
                        print(f"{failcode} got an error")
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
                    elapsed = time.perf_counter() - round_time
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
    # mask = joined_df["Name of Securities"].str.contains("REIT", na=False)
    # joined_df.loc[mask, "Industry_1"] = joined_df.loc[mask, "Sec_name_en"]
    # joined_df.loc[mask, "Sec_name_en"] = ''

    joined_df.drop(columns=[first_col], inplace=True, errors='ignore')
    run_label = time.strftime('%Y%m%d_%H%M')
    handler.save_excel(joined_df, f'output_{run_label}.xlsx')
    elapsed = time.perf_counter() - start
    print(f"Total Time: {elapsed:.2f}s | "
        f"Rate: {len(stock_codes)/elapsed:.1f} stocks/sec")



if __name__ == '__main__':
    run()

