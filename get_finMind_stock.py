import requests
import pandas as pd
import time
from dotenv import load_dotenv
import os

load_dotenv()
FINMIND_API_KEY = os.getenv("FINMIND_API_KEY")

url = "https://api.finmindtrade.com/api/v4/data"
parameter = {
    "dataset": "TaiwanStockInfo",
    "token": FINMIND_API_KEY,
}
resp = requests.get(url, params=parameter)
data = resp.json()
stock_list = pd.DataFrame(data["data"])["stock_id"].tolist()

# 設定查詢區間
START_DATE = "2019-01-01"

# 儲存所有股票的配息資料
all_dividends = []

# 遍歷 stock_list，依序查詢每個股票的配息資料
for stock_id in stock_list:
    try:
        parameter = {
            "dataset": "TaiwanStockDividend",
            "data_id": stock_id,
            "start_date": START_DATE,
            "token": FINMIND_API_KEY,
        }

        response = requests.get(url, params=parameter)
        data = response.json()

        # 確保 API 回傳正確的資料
        if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
            df = pd.DataFrame(data["data"])
            all_dividends.append(df)

        # 防止 API 過載，增加間隔時間
        time.sleep(1)

    except Exception as e:
        print(f"查詢 {stock_id} 失敗: {e}")

# 合併所有配息資料
if all_dividends:
    final_df = pd.concat(all_dividends, ignore_index=True)
    final_df.to_csv("all_stock_dividends.csv", index=False)
    print("所有股票的配息資料已儲存為 all_stock_dividends.csv")
else:
    print("沒有獲取到任何配息資料")