import requests 
from bs4 import BeautifulSoup
import time
import pandas as pd
import sys
import json

def __main__():
    args = sys.argv
    start = int(args[1])
    end = int(args[2])
    INFO_ROWS = [
        "Entity Type:",
        "Operating Status:",
        "Legal Name:", 
        'DBA Name:',
        "Physical Address:",
        "Phone:",
        "MC/MX/FF Number(s):",
        "Power Units:",
        "Drivers:",
        "MCS-150 Form Date:",
        "MCS-150 Mileage (Year):"
    ]
    headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }
    initial_response = requests.get(
        url="https://safer.fmcsa.dot.gov", 
        headers=headers,
        timeout=300
    )
    data={
	"searchtype": "ANY",
	"query_type": "queryCarrierSnapshot",
	"query_param": "MC_MX"}
    records = []

    for company_id in range(start,end+1):
        data["query_string"] = str(company_id)
        response = requests.post("https://safer.fmcsa.dot.gov/query.asp", data=data, headers=headers,cookies=initial_response.cookies, timeout=300)
        record = [company_id]
        if response.status_code == 200:
            html_content = response.content
            try:
                soup = BeautifulSoup(html_content, "html.parser")
                # Find all the text elements (e.g., paragraphs, headings, etc.) you want to scrape
                table_elements= soup.find_all(['a'])
                # Extract the text from each element and concatenate them into a single string
                rows_to_get = [element.parent.nextSibling.nextSibling for element in table_elements if element.get_text() in INFO_ROWS]
                record.extend([element.get_text().strip("\r\t\n\xa0 ").replace("\n", " ").replace("\xa0", "").replace("\t", " ").replace("\r", " ") for element in rows_to_get])
                if record[1] == "CARRIER" and record[2].lower() in ["active","authorized", "authorized for property", "authorized for hhg"]:
                    records.append(record)
            except Exception:
                print(f"Failed for MC_MX: {company_id}")
        else:
            print(f"Failed to fetch record number: {company_id}")
        time.sleep(60)
    dataframe = pd.DataFrame(data=records,columns= ['MC/MX Number', 'Entity Type', 'Operating Status', 'Legal Name', 'DBA Name', 'Physical Address', 'Phone', 'MC/MX/FF Numbers(s)', 'Power Units', 'Drivers', 'MCS-150 Form Date', 'MCS-150 Mileage (Year)'])
    csv_name = f"records_for_mc_mx_{start}_to_{end}"
    try:
        dataframe.to_csv(f"{csv_name}.csv")
    except UnicodeEncodeError:
        try:
            dataframe.to_csv(f"{csv_name}.csv", encoding="utf-8")
        except Exception:
            with open(f"{csv_name}.jsonl", "w") as fp:
                for record in records:
                    json.dump(record, fp)
            
    exit()


__main__()