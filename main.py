import pymupdf
from pymupdf.table import find_tables
import re
from datetime import datetime
import os
import pandas as pd
from tqdm import tqdm


GLOBAL_OFFERING = "hkex_downloads\\Global_Offering"
FINAL = "hkex_downloads\\Allotment_Results"


class Initial:

    def __init__(self, doc_path):
        self.doc_path = doc_path
        self.doc = pymupdf.open(doc_path)
        self.toc = self.doc.get_toc()
        self.info_key = ["sponsor", "Legal Advisor to Company", "Legal Adviser to Company", "Legal sponsor", "accountant", "Receiving Bank"]
        self.listingdate = []
        self.ini = []
        self.appval = []
        self.parties = []
        self.result = []
        self.firstfour = []

    def process(self):
        self.listingdate = self.get_listing_date()
        # self.listingdate = [] if self.listingdate is None else self.listingdate
        self.ini = self.get_ini_offer()
        # self.ini = [] if self.ini is None else self.ini
        self.appval = self.get_table_right_botton()
        # self.appval = [] if self.appval is None else self.appval
        self.firstfour = self.get_first_fournum()
        # self.firstfour = [] if self.firstfour is None else self.firstfour

        labels = self.get_label_info()
        for key in self.info_key:
            info = self.get_company_info_by_label_keyword(labels, key)
            if "Legal" in key:
                self.parties.append({key:info[0] if info else ""})
            else:
                self.parties.append({key:", ".join(info) if info else ""})

        self.result = self.listingdate + self.ini + self.appval + self.firstfour + self.parties

    def get_section_page_range(self, section_title):    
        for i, (level, title, page) in enumerate(self.toc):
            parts = section_title.lower().split()
            index = 0 
            for part in parts:
                if part in title.strip().lower():
                    index += 1
            if index == len(parts):
                start = page -1
                end = self.doc.page_count -1
                for nxt in self.toc[i + 1:]:
                    if nxt[0] <= level:
                        end = nxt[2] - 2
                        break

                return (start, end)
        return None
            

    def get_certain_word_pgno(self, section_title, anchor_text):
        try:
          startpg, endpg = self.get_section_page_range(section_title)
        except Exception:
            return None
        
        normalized_anchor = " ".join(anchor_text.split()).strip('“”"\'')
        anchor_page_idx = None
        anchor_x0 = None
        anchor_y0 = None

        for page_idx in range(startpg, endpg+1):
            page = self.doc[page_idx]
            page_dict = page.get_text("dict")

            found = False

            for block in page_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue

                for line in block.get("lines", []):
                    spans = [s for s in line.get("spans", []) if s.get("text", "").strip()]
                    if not spans:
                        continue

                    text = " ".join(s["text"].strip() for s in spans if s["text"].strip())
                    normalized_text = " ".join(text.split()).strip('“”"\'')
                    if (anchor_text == "Listing Date") and (normalized_anchor in normalized_text) and (len(normalized_text) < 20):
                        anchor_page_idx = page_idx
                        anchor_x0 = min(s["bbox"][0] for s in spans)
                        anchor_y0 = min(s["bbox"][1] for s in spans)
                        found = True
                        break
                    # FULL match only
                    elif normalized_text == normalized_anchor:
                        anchor_page_idx = page_idx
                        anchor_x0 = min(s["bbox"][0] for s in spans)
                        anchor_y0 = min(s["bbox"][1] for s in spans)
                        found = True
                        break


                if found:
                    break

            if found:
                break
        if anchor_page_idx is None:
            raise ValueError(f'Full anchor text "{anchor_text}" not found in the document.')

        end_idx = endpg
        if anchor_page_idx > end_idx:
            return []
        
        return [anchor_page_idx, anchor_x0, anchor_y0]


    #Listing date from "Listing date" in DEF
    def get_listing_date(self, y_tol=30)-> dict:
        LISTING = "Listing Date"
        try:
            anchor = self.get_certain_word_pgno("DEFINITIONS", LISTING)
            pageno = anchor[0]
        except Exception as e:
            return [{"Listing Date": "Not found"}]

        page = self.doc[pageno]
        try:
            label_rect = page.search_for(LISTING)[0]
        except:
            return [{"Listing Date": "cannot find"}]

        words = page.get_text("words")
        same_line = []
        for x0, y0, x1, y1, text, *_ in words:
            if abs(y0 - label_rect.y0) <= y_tol and x0 > label_rect.x1:
                same_line.append([x0, y0, text])

        result = sorted(same_line, key = lambda x: (x[1], x[0]))
        line_text = " ".join(t for _, j, t in result)

        
        match = re.search(
            r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}, \d{4}\b',
            line_text
        )

        
        if match:
            date_str = match.group()
            date_obj = datetime.strptime(date_str, "%B %d, %Y").strftime("%d/%m/%Y")
        else:
            return [{"Listing Date": "cannot find"}]



        return [{"Listing Date": date_obj}]
    
    def get_first_fournum(self):
        try:
            pageno = self.get_section_page_range("Important")[0]
        except:
            pageno =1

        
        page = self.doc[pageno]
        words = page.get_text("words")
        num =[]
        
        def is_number(value):
            try:
                # remove commas (thousands separators)
                cleaned = value.replace(',', '')
                float(cleaned)
                return True
            except ValueError:
                return False
            
        for x0, y0, x1, y1, text, *_ in words:
            if is_number(text):
                num.append(text)
                # print(text)
            if len(num) == 4:
                return([{"number":num}])
        return [{"number": "None"}]

    #Offer share, offer price
    def get_ini_offer(self, pageno =1, y_tol = 3):
        
        LABELS = [
            "Number of Offer Shares under the Global Offering",
            "Number of Hong Kong Offer Shares",
            "Number of International Offer Shares",
            "Price",
            "Stock Code"
        ]
        try:
            pageno = self.get_section_page_range("Important")[0]
        except:
            pageno =1

        page = self.doc[pageno]
        lines = []
        result = []
        words = page.get_text("words")
        # print(words)


        for label in LABELS:
            rects = page.search_for(label)
                
            
            if not rects:
                lines.append("Label not found")
                continue

            label_rect = rects[0]

            same_line = []
            for word in words:
                if abs(word[1] - label_rect.y0) <= y_tol and word[0] > label_rect.x1:
                    same_line.append((word[0], word[4]))

            same_line.sort()
            line_text = " ".join(t for _, t in same_line)
            lines.append(line_text)

        for i,line in enumerate(lines):
            try:
                match = re.search(r'\b\d{1,5}(?:,\d{3})*(?:\.\d+)?\b', line)
                result.append({"Initial "+LABELS[i]: match.group()})
            except:
                result.append({"Initial "+LABELS[i]: "No match"})

        return result

    #Application value of the top tier (last page of important)
    def get_table_right_botton(self, x_tol=10):
        try:
            pageno = self.get_section_page_range("important")[1]
        except:
            return [{"Application value of the Top Tier" : "None"}]
        page = self.doc[pageno]
        words = page.get_text("words")
        columns = {}

        for x0, y0, x1, y1, text, *_ in words:
            is_num = re.fullmatch(r"\d+(?:[^\w\s]+\d+)*", text)
            if not is_num:
                continue

            # cluster x positions into columns
            x_key = round(x1 / x_tol) * x_tol
            columns.setdefault(x_key, []).append((y0, text))

        if columns:
            rightmost = max(columns.keys())
            col = columns[rightmost]
            col.sort(key=lambda x:x[0])
            return [{"Application value of the Top Tier" : col[-1][1]}]
        else:
            return [{"Application value of the Top Tier" : "None"}]


    #page numbers are 0 based
    def get_label_info(self, x_tol = 30):
        anchor_text = "PARTIES INVOLVED IN THE GLOBAL OFFERING"
        section = "DIRECTORS AND PARTIES INVOLVED IN THE GLOBAL OFFERING"

        try:
            startpg, endpg = self.get_section_page_range(section)
        except TypeError:
            return []
            
        try:
            anchor = self.get_certain_word_pgno(section, anchor_text)
        except Exception:
            return []
        
        anchor_page_idx = anchor[0]
        anchor_x0 = anchor[1]
        anchor_y0 = anchor[2]

        results = []

        
            
        for page_idx in range(anchor_page_idx, endpg+1):
            page = self.doc[page_idx]
            page_dict = page.get_text("dict")

            for block in page_dict.get("blocks", []):

                if block.get("type") != 0:
                    continue

                for line in block.get("lines", []):
                    spans = [s for s in line.get("spans", []) if s.get("text", "").strip()]
                    if not spans:
                        continue

                    text = " ".join(s["text"].strip() for s in spans if s["text"].strip())
                    x0 = min(s["bbox"][0] for s in spans)
                    y0 = min(s["bbox"][1] for s in spans)
                    x1 = max(s["bbox"][2] for s in spans)
                    y1 = max(s["bbox"][3] for s in spans)

                    if not text:
                        continue

                    # skip the anchor heading itself
                    if anchor_text in text:
                        continue

                    # on the anchor page, only keep lines below the anchor heading
                    if page_idx == anchor_page_idx and y0 <= anchor_y0:
                        continue

                    
                    if abs(x0 - anchor_x0) <= x_tol:
                        # print(line)
                        results.append({
                            "page": page_idx,
                            "text": text,
                            "x0": x0,
                            "y0": y0,
                            "x1": x1,
                            "y1": y1,
                        })


        
        results.sort(key=lambda item: (item["page"], item["y0"]))

        filtered_results = []
        prev = None


        for item in results:

            if not filtered_results:
                filtered_results.append(item)
                prev = item

                continue


            same_page = item["page"] == prev["page"]
            close_y = abs(item["y0"] - prev["y0"]) <= 25
            prev = item


            if same_page and close_y:
                filtered_results[-1]["text"] += (" " + item["text"])
                if item["x1"] > filtered_results[-1]["x1"]:
                    filtered_results[-1]["x1"] = item["x1"]          
                if item["y1"] > filtered_results[-1]["y1"]:
                    filtered_results[-1]["y1"] = item["y1"]          
                continue

            filtered_results.append(item)

        return filtered_results

    def alt_comp_info(self, startpg, endpg, key):
        section = "PARTIES INVOLVED IN THE OFFER"
        keyword_token = set(re.findall(r"\b\w+\b", key))
        results = []
        for pg in range(startpg, endpg+1):
            page = self.doc[pg]
            page_dict = page.get_text("dict")

            for block in page_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue

                for line in block.get("lines", []):
                    spans = [s for s in line.get("spans", []) if s.get("text", "").strip()]
                    if not spans:
                        continue

                    text = " ".join(s["text"].strip() for s in spans if s["text"].strip())
                    x0 = min(s["bbox"][0] for s in spans)
                    y0 = min(s["bbox"][1] for s in spans)
                    x1 = max(s["bbox"][2] for s in spans)
                    y1 = max(s["bbox"][3] for s in spans)

                    if not text:
                        continue


                    results.append({
                            "page": pg,
                            "text": text,
                            "x0": x0,
                            "y0": y0,
                            "x1": x1,
                            "y1": y1,
                        })
        results.sort(key=lambda item: (item["page"], item["y0"]))
        # print(results)
        minx0 = min(d["x0"] for d in results)
        newresult = [d for d in results if d["x0"] - minx0 <= 30]
        newresult.sort(key=lambda item: (item["page"], item["y0"]))

        filtered_results = []
        prev = None


        for item in newresult:

            if not filtered_results:
                filtered_results.append(item)
                prev = item

                continue


            same_page = item["page"] == prev["page"]
            close_y = abs(item["y0"] - prev["y0"]) <= 25
            prev = item


            if same_page and close_y:
                filtered_results[-1]["text"] += (" " + item["text"])
                if item["x1"] > filtered_results[-1]["x1"]:
                    filtered_results[-1]["x1"] = item["x1"]          
                if item["y1"] > filtered_results[-1]["y1"]:
                    filtered_results[-1]["y1"] = item["y1"]          
                continue

            filtered_results.append(item)
        # print(filtered_results)



        return filtered_results


    def get_company_info_by_label_keyword(self,labels_info,label_keyword):
        section = "PARTIES INVOLVED IN THE OFFER"
        keyword = label_keyword.strip().lower()
        alt = False
        if not labels_info:
            try:
                startpg, endpg = self.get_section_page_range(section)
                aftersort = self.alt_comp_info(startpg, endpg, keyword)
                alt = True
            except TypeError:
                return []

        if not keyword:
            return []

        keyword_token = set(re.findall(r"\b\w+\b", keyword))

        # Sort labels by page then y
        if not alt:
            sorted_labels = sorted(labels_info, key=lambda item: (item["page"], item["y0"]))
        else:
            sorted_labels = aftersort

        matched_labels = []

        for item in sorted_labels:
            text = str(item.get("text", "")).strip().lower()
            words = re.findall(r"\b\w+\b", text)
            text_token = set([w[:-1] if w.endswith("s") else w for w in words])
            if keyword_token.issubset(text_token):
                matched_labels.append(item)

        
        if keyword == "sponsor":
            matched_labels = [
                        l for l in matched_labels
                        if "legal" not in l["text"].lower()
                    ]
        if not matched_labels:
            return []
        page = self.doc[matched_labels[0]["page"]]


        search = []
        for label in matched_labels:
            page_num = label["page"]
            startx0 = label["x1"]
            starty0 = label["y0"]
            endx1 = page.rect.width
            index_in_sorted = sorted_labels.index(label)
            if index_in_sorted != (len(sorted_labels) -1):
                next = sorted_labels[index_in_sorted+1]
                if next["page"] != page_num:
                    search.append((startx0, starty0, endx1, page.rect.height, label["page"]))
                    search.append((startx0, 0, endx1, next["y0"], next["page"]))
                else:
                    search.append((startx0, starty0, endx1, next["y0"], label["page"]))
            else:
                search.append((startx0, starty0, endx1, page.rect.height, label["page"]))
        results = []
        for i,rect in enumerate(search):
            clip_rect = pymupdf.Rect(rect[0], rect[1], rect[2], rect[3])
            page = self.doc[rect[4]]
            page_dict = page.get_text("dict", clip=clip_rect)
            lines = []

            has_bold = False
            for block in page_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue
                    
                for line in block.get("lines", []):
                    check = [s for s in line.get("spans", []) if (s.get("text", "").strip()) and "bold" in s["font"].lower()]
                    if len(check) != 0:
                        has_bold = True
                        break
                if has_bold:
                    break


            for block in page_dict.get("blocks", []):
                if block.get("type") != 0:
                    continue
                

                for line in block.get("lines", []):
                    if has_bold:
                        spans = [s for s in line.get("spans", []) if (s.get("text", "").strip()) and "bold" in s["font"].lower()]
                    else:
                        spans = [s for s in line.get("spans", []) if (s.get("text", "").strip())]

                    if not spans:
                        continue

                    line_text = " ".join(s["text"].strip() for s in spans if s["text"].strip())
                    y0 = min(s["bbox"][1] for s in spans)
                    x0 = min(s["bbox"][0] for s in spans)
                    if re.match(r"–\s*\d+\s*–", line_text):
                        continue
                    
                    if line_text:
                        lines.append({
                            "text": line_text,
                            "y": y0,
                            "x": x0
                        })
            lines.sort(key=lambda item: (item["y"], item["x"]))
            if not lines:
                continue
            full_comp = []
            prev_y = 0
            for line in lines:
                if not full_comp:
                    prev_y = line["y"]
                    full_comp.append(line["text"])
                    continue
                if abs(line["y"] - prev_y) < 30:
                    full_comp.append(line["text"])
                    prev_y = line["y"]
                    
                else:
                    comp = " ".join([i.strip() for i in full_comp])
                    if comp.isupper() and comp != "KPMG":
                        continue
                    else:
                        results.append(comp)
                    full_comp = []
                    full_comp.append(line["text"])
                    prev_y = line["y"]
            comp = " ".join([i.strip() for i in full_comp])
            if comp.isupper() and comp != "KPMG":
                continue
            else:
                results.append(comp)



        return list(dict.fromkeys(results))

class Final:
    def __init__(self, doc_path):
        self.path = doc_path
        self.doc = pymupdf.open(doc_path)
        self.offer = []
        self.application = []
        self.result = []


    def process(self):
        self.offer = self.get_fin_offer() 
        self.application = self.get_application()
        try:
            self.result = self.offer + self.application
        except Exception as e:
            print(self.offer, self.application)


    #Get final gloabal and public offer    
    def get_fin_offer(self, pageno=1, y_tol = 3):
        
        LABELS = [
            "Number of Offer Shares under the Global",
            "Number of Hong Kong Offer Shares",
            "Stock Code"
        ]
        target = "Stock Code"
        pageno = 3
        for i, page in enumerate(self.doc):
            if page.search_for(target.lower()):
                pageno = i
                break
        
        page = self.doc[pageno]
        lines = []
        result = []



        for label in LABELS:
            rects = page.search_for(label)
            if not rects:
                lines.append("Not found")
                continue

            label_rect = rects[0]
            words = page.get_text("words")

            same_line = []
            for x0, y0, x1, y1, text, *_ in words:
                if abs(y0 - label_rect.y0) <= y_tol and x0 > label_rect.x1:
                    same_line.append((x0, text))

            same_line.sort()
            line_text = " ".join(t for _, t in same_line)
            lines.append(line_text)

        for i,line in enumerate(lines):
            match = re.search(r'\b\d{1,5}(?:,\d{3})*(?:\.\d+)?\b', line)
            result.append({"Final "+LABELS[i] : match.group() if match else "None"})
        

        return result

    def get_application(self):
        labels = ["valid applications", "successful applications", "Subscription level"]
        target = "Allotment Results Details"
        pageno = 3
        for i, page in enumerate(self.doc):
            if page.search_for(target.lower()):
                pageno = i
                break

        rects = []
        for pgno in range(pageno, pageno+2):
            if len(rects) == 3:
                break
            page = self.doc[pgno]
            for i, label in enumerate(labels):
                rect = page.search_for(label)
                if len(rect) != 0:
                    rects.append((rect[0], pgno))
                # else:
                #     rects.append(None)

        result = []
        for i, rect in enumerate(rects):
            # if not rect:
            #     result.append({labels[i] : "None"})
            #     continue
            page = self.doc[rect[1]]
            page_words = page.get_text("words")
            group = []
            for x0, y0, x1, y1, word, *_ in page_words:
                try:
                    if abs(y0 - rect[0][1]) < 5 and x0 > rect[0][2]:
                        group.append((x0, word))
                except:
                    group.append((0, "None"))
                    break
            group.sort(key = lambda x: x[0])
            text = " ".join([j[1].strip() for j in group])
            match = re.search(r'([\d,]+(?:\.\d+)?)\s*', text)


            result.append({labels[i] : match.group(1)})

        return result


def main():
    #path
    global_offer_path = []
    final_allot_path = []

    #big list of datas
    global_list = []
    final_list = []


    #get file path
    for filename in os.listdir(GLOBAL_OFFERING):
        full_path = os.path.join(GLOBAL_OFFERING, filename)
        if os.path.isfile(full_path):
            global_offer_path.append(full_path)

    global_offer_path = sorted([os.path.join(GLOBAL_OFFERING, f) for f in os.listdir(GLOBAL_OFFERING) if os.path.isfile(os.path.join(GLOBAL_OFFERING, f))])
    for filename in os.listdir(FINAL):
        full_path = os.path.join(FINAL, filename)
        if os.path.isfile(full_path):
            final_allot_path.append(full_path)

    final_allot_path = sorted([os.path.join(FINAL, f) for f in os.listdir(FINAL) if os.path.isfile(os.path.join(FINAL, f))])
    #combine into one big list for each
    for i, path in enumerate(tqdm(global_offer_path)):
        inifile = Initial(path)
        try:
            inifile.process()
            global_list.append(inifile.result)
        except Exception as e:
            print(f"\nInitial: {e, path}")


    for i, path in enumerate(tqdm(final_allot_path)):
        finfile = Final(path)
        try:
            finfile.process()
            final_list.append(finfile.result)
        except Exception as e:
            print(f"\nFinal:{e, path}")
    # print(global_list)
    # print(final_list)


    dfini = pd.DataFrame(
                {k: v for d in row for k, v in d.items()}
                for row in global_list
            )
    a = "Legal Advisor to Company"
    b = "Legal Adviser to Company"
    dfini["Legal advisor"] = dfini.apply(lambda row: row[a] if row[a] else row[b], axis = 1)
    dfini = dfini.drop(columns=[a, b])
    col1 = dfini.pop("Legal advisor")
    dfini.insert(9, "Legal advisor", col1)
    # dfini = dfini.rename(columns={'Initial Stock Code': "Stock Code"})

    dffin = pd.DataFrame({k: v for d in row for k, v in d.items()}
                for row in final_list
            )
    
    # dffin = dffin.rename(columns={'Final Stock Code': "Stock Code"})
    dfini['Initial Stock Code'] = dfini['Initial Stock Code'].str.lstrip('0')
    dffin['Final Stock Code'] = dffin['Final Stock Code'].str.lstrip('0')

    df = pd.merge(dfini, dffin, left_on="Initial Stock Code", right_on="Final Stock Code", how="outer")
    # col2 = df.pop("Stock Code")
    # df.insert(0, "Stock code", col2)

    df.to_excel("output.xlsx", index = False)

    print("Done")


if __name__ == "__main__":
    main()        
    input("Press Enter to exit...")    

