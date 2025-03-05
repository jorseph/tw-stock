import requests
import pandas as pd
import unittest
import json
from dbHelper import save_stock_list, load_stock_list, init_db


def fetch_stock_data(date: str, stock_no: str):
    """
    取得指定日期與股票代碼的殖利率、本益比、股價淨值比等數據。

    :param date: 查詢日期 (格式: YYYYMMDD)
    :param stock_no: 股票代碼 (如 2330)
    :return: DataFrame 或 None
    """
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date={date}&stockNo={stock_no}&response=json"
    
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"⚠️ 無法取得資料，HTTP 狀態碼: {response.status_code}")
        return None

    data = response.json()

    # 🔹 Debug: 印出 API 回應內容
    print("🔍 API 回應內容:")
    print(json.dumps(data, indent=4, ensure_ascii=False))  # 讓 JSON 更易讀
    
    if "data" not in data or not data["data"]:
        print("⚠️ 找不到符合條件的資料")
        return None

    # 解析欄位名稱
    columns = data["fields"]
    
    # 解析數據內容
    df = pd.DataFrame(data["data"], columns=columns)

    return df


def fetch_latest_financial_report(stock_no: str, year: int, month: int):
    """
    取得指定月份最新的財報數據 (第一筆新財報數據)
    
    :param stock_no: 股票代碼 (如 2330)
    :param year: 查詢年 (西元年，例如 2025)
    :param month: 查詢月份 (2, 5, 8, 11)
    :return: DataFrame (單筆數據) 或 None
    """
    date_str = f"{year}{month:02}01"  # 例如 20250201
    url = f"https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU?date={date_str}&stockNo={stock_no}&response=json"
    
    headers = {"User-Agent": "Mozilla/5.0"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"⚠️ 無法取得資料，HTTP 狀態碼: {response.status_code}")
        return None

    data = response.json()
    
    if "data" not in data or not data["data"]:
        print("⚠️ 找不到符合條件的資料")
        return None

    # 解析欄位名稱
    columns = data["fields"]
    
    # 解析數據內容
    df = pd.DataFrame(data["data"], columns=columns)

    # 找出第一筆「新的財報年/季」資料
    first_new_financial_report = df.iloc[0]  # 取第一筆數據

    return first_new_financial_report.to_frame().T  # 回傳 DataFrame (單筆)


def calculate_roe(financial_data):
    """
    計算 ROE (Return on Equity) 每一季數據
    
    :param financial_data: DataFrame 包含財報數據
    :return: DataFrame 包含 ROE 計算結果
    """
    # 確保數據存在
    if financial_data is None or financial_data.empty:
        print("⚠️ 沒有財報數據，無法計算 ROE")
        return None
    
    # 嘗試將數據轉換為數值類型
    financial_data["本益比"] = pd.to_numeric(financial_data["本益比"], errors='coerce')
    financial_data["股價淨值比"] = pd.to_numeric(financial_data["股價淨值比"], errors='coerce')

    # 計算 ROE
    financial_data["ROE (%)"] = (1 / financial_data["股價淨值比"]) * 100  # ROE 近似值

    return financial_data


def fetch_stock_list():
    """從 API 抓取股票列表，只執行一次"""
    existing_list = load_stock_list()

    if existing_list:
        print("📂 股票列表已存在，直接從資料庫讀取")
        return existing_list

    url = "https://quality.data.gov.tw/dq_download_json.php?nid=11549&md5_url=bb878d47ffbe7b83bfc1b41d0b24946e"  # 假設 API 網址
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"⚠️ API 錯誤: {response.status_code}")
        return None

    data = response.json()

    stock_list = [(item["stock_no"], item["name"], item["industry"], item["market"]) for item in data]

    # 存入資料庫
    save_stock_list(stock_list)
    
    print("✅ 股票列表已成功存入資料庫")
    return stock_list


if __name__ == "__main__":
    init_db()  # 確保資料庫初始化
    stock_list = fetch_stock_list()

    if stock_list:
        print("\n📊 股票列表:")
        for stock in stock_list[:10]:  # 只顯示前 10 檔
            print(stock)
