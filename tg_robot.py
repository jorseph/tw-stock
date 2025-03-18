import pandas as pd
import logging
import os
import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 設定日誌
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

print("當前工作目錄:", os.getcwd())

import pandas as pd
from telegram import Update
from telegram.ext import CallbackContext

load_dotenv()
FINMIND_API_KEY = os.getenv("FINMIND_API_KEY")
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"

# 讀取股票基本資訊 CSV
CSV_FILE = "Calculated_Stock_Values.csv"
df = pd.read_csv(CSV_FILE)

# 確保 "代號" 欄位為字串
df["代號"] = df["代號"].astype(str)

# 讀取配息資訊 CSV
DIVIDEND_CSV_FILE = "all_stock_dividends.csv"
df_dividend = pd.read_csv(DIVIDEND_CSV_FILE)

# 確保 "stock_id" 欄位為字串
df_dividend["stock_id"] = df_dividend["stock_id"].astype(str)

# 確保 "CashEarningsDistribution" 欄位是數值類型（避免 NaN 問題）
df_dividend["CashEarningsDistribution"] = pd.to_numeric(df_dividend["CashEarningsDistribution"], errors='coerce')

# 處理查詢指令
async def stock(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入股票代號，例如：/stock 3008")
        return

    stock_id = context.args[0]
    stock_info = df[df["代號"] == stock_id]

    if stock_info.empty:
        await update.message.reply_text(f"找不到股票代號 {stock_id} 的資訊")
        return

    info = stock_info.iloc[0]  # 取得第一筆股票資訊

    # 🔹 回應訊息
    message = (
        f"📊 **股票資訊 - {info['名稱']} ({stock_id})**\n"
        f"🔹 **成交價**: {info['成交']} 元\n"
        f"📉 **最低合理股價**: {info['最低合理股價']:.2f} 元\n"
        f"📊 **平均合理股價**: {info['平均合理股價']:.2f} 元\n"
        f"📈 **最高合理股價**: {info['最高合理股價']:.2f} 元\n"
        f"📊 **平均財報評分**: {info['平均財報評分']:.2f}\n"
        f"📊 **平均ROE(%)**: {info['平均ROE(%)']:.2f}%\n"
        f"📊 **平均ROE增減**: {info['平均ROE增減']:.2f}\n"
        f"💰 **淨利成長(%)**: {info['淨利成長(%)']:.2f}%\n"
        f"🏦 **平均淨利(%)**: {info['平均淨利(%)']:.2f}%\n\n"
    )

    await update.message.reply_text(message, parse_mode="Markdown")


# 推薦前 15 筆股票（根據平均財報評分排序）
async def recommend(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入推薦股數，最多20 例如：/recommend 15")
        return

    try:
        count = int(context.args[0])  # 轉換成整數
        if count <= 0:
            await update.message.reply_text("請輸入大於 0 的數量，例如：/recommend 10")
            return
        if count > 15:
            await update.message.reply_text("最多只能推薦 15 檔股票，請輸入小於等於 15 的數字")
            return
    except ValueError:
        await update.message.reply_text("請輸入有效的數字，例如：/recommend 10")
        return

    # 處理 "淨利成長(%)" 欄位
    df["淨利成長(%)"] = df["淨利成長(%)"].astype(str).str.replace("%", "")
    df["淨利成長(%)"] = df["淨利成長(%)"].apply(lambda x: 0 if "-" in x else float(x.replace("+", "")))

    # 處理 "平均淨利(%)" 欄位
    df["平均淨利(%)"] = df["平均淨利(%)"].astype(str).str.replace("%", "")
    df["平均淨利(%)"] = df["平均淨利(%)"].apply(lambda x: 0 if "-" in x else float(x.replace("+", "")))

    # 檢查清理後的數據
    print(df[["淨利成長(%)", "平均淨利(%)"]].head())

    df["淨利成長(%)"] = pd.to_numeric(df["淨利成長(%)"], errors="coerce")
    df["平均淨利(%)"] = pd.to_numeric(df["平均淨利(%)"], errors="coerce")
    # 過濾條件：
    # - 目前成交價 < 最低合理股價
    # - 淨利成長(%) > 0
    # - 平均淨利(%) > 10
    top_stocks = df[
        (df["成交"] < df["最低合理股價"]) & 
        (df["淨利成長(%)"] > 0.0) &
        (df["平均淨利(%)"] > 10.0) & 
        (df["平均ROE(%)"] > 10) &  # ROE 超過10%
        (df["營收成長(%)"].abs() < 10) &  # 營收波動不超過10%
        (df["平均ROE增減"] > 0) &  # 平均ROE增減 > 0
        (df["平均毛利(%)"] > 30) &  # 毛利率 > 30%
        (df["統計年數_x"] > 5)  # 淨利率 > 10%
    ].sort_values(by="平均財報評分", ascending=False).head(count)

    message = f"📢 **推薦股票前 {count} 名（依平均財報評分）**\n"
    for _, stock in top_stocks.iterrows():
        message += (
            f"🔹 **{stock['名稱']} ({stock['代號']})**\n"
            f"   📊 **評分**: {stock['平均財報評分']:.2f} | 📈 **成交價**: {stock['成交']} 元\n"
            f"   📉 **最低合理**: {stock['最低合理股價']:.2f} | 📊 **平均合理**: {stock['平均合理股價']:.2f}\n"
            f"   💰 **淨利成長(%)**: {stock['淨利成長(%)']:.2f}% | 🏦 **平均淨利(%)**: {stock['平均淨利(%)']:.2f}%\n\n"
        )

    await update.message.reply_text(message, parse_mode="Markdown")

# 設定機器人
async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("歡迎使用股票查詢機器人！請輸入 /stock <股票代號> 或 /recommend")


# Telegram Bot 指令：/stock_estimate 2330
async def stock_estimate(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入股票代號，例如：/stock_estimate 2330")
        return

    stock_id = context.args[0]
    df_result = calculate_quarterly_stock_estimates(stock_id)

    if df_result is None:
        await update.message.reply_text(f"⚠️ 無法獲取 {stock_id} 的數據，請檢查 API 設定或股票代號")
        return

    # 取最近 4 季數據
    df_result = df_result.tail(4)

    # 生成回應訊息
    message = f"📊 **{stock_id} 季度 ROE & 推估股價** 📊\n"
    for _, row in df_result.iterrows():
        message += (
            f"\n📅 **季度**: {row['quarter']}"
            f"\n📊 **ROE**: {row['ROE']:.2f}%"
            f"\n🏦 **BVPS**: {row['BVPS']:.2f} 元"
            f"\n💰 **推估EPS**: {row['推估EPS']:.2f} 元\n"
            f"\n💰 **正常股價**: {row['正常股價']:.2f} 元\n"
            f"\n💰 **低股價**: {row['低股價']:.2f} 元\n"
            "--------------------"
        )

    await update.message.reply_text(message, parse_mode="Markdown")


async def etf(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入 ETF 代號，例如：/etf 00713")
        return
    
    # 🔹 查詢當前股價
    stock_id = context.args[0]
    current_price = get_current_stock_price(stock_id)

    if current_price is None:
        await update.message.reply_text(f"無法獲取 {stock_id} 的最新股價，請稍後再試")
        return

    # 🔹 計算最近一年配息總額 & 殖利率
    # total_dividends, dividend_yield = calculate_dividend_yield(stock_id, current_price)
    total_dividends, dividend_yield = calculate_all_dividend_yield(stock_id, current_price)

    # 🔹 回應訊息
    message = (
        f"📊 **ETF 資訊 - {stock_id}**\n"
        f"🔹 **當前股價**: {current_price:.2f} 元\n"
        f"💸 **最近一年配息總額**: {total_dividends:.2f} 元 💰\n"
        f"📊 **殖利率**: {dividend_yield:.2f}%\n"
    )
    
    await update.message.reply_text(message, parse_mode="Markdown")


# 🔹 查詢最近的交易日股價
def get_current_stock_price(stock_id):
    # 設定最大回溯天數，避免過度請求 API
    max_days = 5  
    check_date = datetime.today() - timedelta(days=1)  # 預設查詢前一天

    for _ in range(max_days):
        # 格式化日期
        start_date = check_date.strftime('%Y-%m-%d')

        parameter = {
            "dataset": "TaiwanStockPrice",
            "data_id": stock_id,
            "start_date": start_date,
            "token": FINMIND_API_KEY,
        }

        response = requests.get(FINMIND_URL, params=parameter)
        data = response.json()

        # 檢查 API 回應是否有數據
        if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
            df_price = pd.DataFrame(data["data"])
            latest_price = df_price.sort_values(by="date", ascending=False).iloc[0]["close"]
            return latest_price  # 找到最近的股價後返回

        # 如果沒有數據，向前推一天（避免週六日查不到）
        check_date -= timedelta(days=1)

    return None  # 若 5 天內都查不到股價則回傳 None
    

def calculate_dividend_yield(stock_id, current_price):
    """ 計算該 ETF 或股票的最近一年度配息總額，並計算殖利率 """
    
    # 過濾特定股票
    stock_dividends = df_dividend[(df_dividend["stock_id"] == stock_id) & (df_dividend["CashEarningsDistribution"] > 0)].copy()

    # 確保 date 欄位是 datetime 格式
    stock_dividends["date"] = pd.to_datetime(stock_dividends["date"], errors="coerce")

    if stock_dividends.empty:
        return 0.0, 0.0  # 如果該股票無配息資料，則回傳 0

    # 🔹 取得最近一年的配息
    one_year_ago = datetime.today() - timedelta(days=365)
    
    # **這行錯誤的比較改為確保 date 欄位是 datetime**
    last_year_dividends = stock_dividends[stock_dividends["date"] >= one_year_ago]

    # 計算年度配息總額
    total_dividends = last_year_dividends["CashEarningsDistribution"].sum()

    # 計算殖利率
    if current_price > 0:
        dividend_yield = (total_dividends / current_price) * 100
    else:
        dividend_yield = 0.0

    return total_dividends, dividend_yield


# 🔹 查詢配息資料並計算完整殖利率
def calculate_all_dividend_yield(stock_id, current_price):
    """ 
    計算完整殖利率（包含現金與股票股利） 
    """
    print(f"\n📌 **DEBUG: 開始計算 {stock_id} 的完整殖利率**")
    print(f"🔹 當前股價: {current_price}")

    # 🔹 過濾該股票的配息資料
    stock_dividends = df_dividend[df_dividend["stock_id"] == stock_id].copy()
    print(f"🔹 股票 {stock_id} 配息資料筆數: {len(stock_dividends)}")

    # 確保 date 欄位為 datetime 格式
    stock_dividends["date"] = pd.to_datetime(stock_dividends["date"], errors="coerce")
    
    if stock_dividends.empty:
        print("⚠️ 無配息資料，回傳 0")
        return 0.0, 0.0  # 如果該股票無配息資料，則回傳 0

    # 🔹 取得最近一年的配息
    one_year_ago = datetime.today() - timedelta(days=365)
    last_year_dividends = stock_dividends[stock_dividends["date"] >= one_year_ago]
    print(f"🔹 過濾最近一年的配息資料筆數: {len(last_year_dividends)}")
    
    # 確保至少有 1 筆配息資料
    if last_year_dividends.empty:
        print("⚠️ 最近一年無配息資料，回傳 0")
        return 0.0, 0.0

    # 計算最近一年的 **現金股利總額**
    total_cash_dividends = last_year_dividends["CashEarningsDistribution"].sum()
    print(f"💵 總現金股利: {total_cash_dividends:.4f} 元")

    # 計算最近一年的 **股票股利總額**
    total_stock_dividends = last_year_dividends["StockEarningsDistribution"].sum()
    print(f"📈 總股票股利: {total_stock_dividends:.4f} 股")

    # **計算除權息後股價**
    ex_rights_price = max(current_price - total_cash_dividends, 0)  # 確保股價不為負
    print(f"📉 除權息後股價: {ex_rights_price:.4f} 元")

    # **計算股票股利價值**
    stock_dividend_value = total_stock_dividends * ex_rights_price
    print(f"💹 股票股利價值: {stock_dividend_value:.4f} 元")

    # **計算總股利價值**
    total_dividend_value = stock_dividend_value + (total_cash_dividends * 1000)
    print(f"💰 總股利價值: {total_dividend_value:.4f} 元")

    # **計算還原殖利率**
    if current_price > 0:
        restored_dividend_yield = (total_dividend_value / current_price) * 1000
    else:
        restored_dividend_yield = 0.0
    print(f"📊 **還原殖利率: {restored_dividend_yield:.4f}%**")

    return total_dividend_value, restored_dividend_yield


# 計算季度 ROE & 推估股價
def calculate_quarterly_stock_estimates(stock_id, start_date="2020-01-01", end_date="2025-12-31"):
    """ 透過 FinMind API 取得 PBR、PER，計算季度 ROE、BVPS、推估股價 """
    parameter = {
        "dataset": "TaiwanStockPER",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": FINMIND_API_KEY,
    }

    response = requests.get(FINMIND_URL, params=parameter)
    data = response.json()

    if "data" not in data or not isinstance(data["data"], list) or len(data["data"]) == 0:
        print("❌ 無法獲取數據，請檢查 API 設定或股票代號")
        return None

    df = pd.DataFrame(data["data"])

    # 確保數據格式
    df["date"] = pd.to_datetime(df["date"])
    df["PBR"] = pd.to_numeric(df["PBR"], errors="coerce")
    df["PER"] = pd.to_numeric(df["PER"], errors="coerce")

    print("\n📌 **數據轉換後 (日期轉換 & 數值處理後)**")
    print(df.tail())

    # 計算 ROE (%)
    df["ROE"] = (df["PBR"] / df["PER"]) * 100

    # 依季度取數據
    df["quarter"] = df["date"].dt.to_period("Q")
    
    # **計算季度 PER 平均值 & 最低值**
    df_per_stats = df.groupby("quarter")["PER"].agg(["mean", "min"]).reset_index()
    df_per_stats.rename(columns={"mean": "PER_平均值", "min": "PER_最低值"}, inplace=True)

    df_quarterly = df.groupby("quarter").last().reset_index()

    # **合併 PER 統計數據**
    df_quarterly = df_quarterly.merge(df_per_stats, on="quarter", how="left")

    print("\n📌 **季度數據 (每季最後一天的數據)**")
    print(df_quarterly.tail())

    # 🔹 **計算 BVPS**
    df_quarterly["prev_close"] = get_current_stock_price(stock_id)
    df_quarterly["BVPS"] = df_quarterly["prev_close"] / df_quarterly["PBR"]

    # 🔹 **計算推估股價**
    df_quarterly["推估EPS"] = (df_quarterly["ROE"] / 100) * df_quarterly["BVPS"]

    # 🔹 **計算正常股價（PER 平均值 × BVPS）**
    df_quarterly["正常股價"] = df_quarterly["PER_平均值"] * df_quarterly["推估EPS"]

    # 🔹 **計算低股價（PER 最低值 × BVPS）**
    df_quarterly["低股價"] = df_quarterly["PER_最低值"] * df_quarterly["推估EPS"]

    print("\n📌 **計算推估股價 之後**")
    print(df_quarterly[["quarter", "stock_id", "ROE", "BVPS", "PER", "推估EPS", "正常股價", "低股價"]].tail())

    return df_quarterly


def main():
    load_dotenv()  # 載入 .env 變數
    
    # 讀取 Heroku 環境變數
    BOT_TOKEN = os.getenv("BOT_TOKEN")

    if not BOT_TOKEN:
        raise ValueError("未找到 BOT_TOKEN，請在 Heroku 環境變數設定 BOT_TOKEN")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stock", stock))
    app.add_handler(CommandHandler("recommend", recommend))
    app.add_handler(CommandHandler("etf", etf))
    app.add_handler(CommandHandler("stock_estimate", stock_estimate))

    app.run_polling()

if __name__ == "__main__":
    main()
