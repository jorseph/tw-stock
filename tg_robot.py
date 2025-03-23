import pandas as pd
import logging
import os
import requests
import signal
import sys
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np
import asyncio
import json
import aiohttp
import time

# 設定日誌
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# 創建一個全局的 application 變量
app = None

# 添加全局變量來追蹤執行狀態
is_processing = False
current_task = None
should_cancel = False  # 新增取消標記

# 信號處理函數
def signal_handler(signum, frame):
    logger.info("收到終止信號，正在優雅退出...")
    if app:
        logger.info("正在停止 Telegram Bot...")
        app.stop()
    sys.exit(0)

# print("當前工作目錄:", os.getcwd())

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

# 設定機器人
async def start(update: Update, context: CallbackContext) -> None:
    await update.message.reply_text("歡迎使用股票查詢機器人！請輸入 /stock <股票代號> 或 /recommend")


# Telegram Bot 指令：/stock_estimate 2330
async def stock_estimate(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入股票代號，例如：/stock_estimate 2330")
        return

    stock_id = context.args[0]
    df_result = await calculate_quarterly_stock_estimates(stock_id)

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
            f"\n💰 **推估EPS**: {row['推估EPS']:.2f} 元"
            f"\n📈 **PER 區間**: {row['PER_最低值']:.2f} ~ {row['PER_最高值']:.2f}"
            f"\n📉 **低股價**: {row['低股價']:.2f} 元"
            f"\n📊 **正常股價**: {row['正常股價']:.2f} 元"
            f"\n📈 **高股價**: {row['高股價']:.2f} 元"
            f"\n--------------------"
        )

    await update.message.reply_text(message, parse_mode="Markdown")


async def etf(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入 ETF 代號，例如：/etf 00713")
        return
    
    # 🔹 查詢當前股價
    stock_id = context.args[0]
    current_price = await get_current_stock_price(stock_id)

    if current_price is None:
        await update.message.reply_text(f"無法獲取 {stock_id} 的最新股價，請稍後再試")
        return

    # 🔹 計算最近一年配息總額 & 殖利率
    # total_dividends, dividend_yield = calculate_dividend_yield(stock_id, current_price)
    total_dividends, dividend_yield, dividends_count = calculate_all_dividend_yield(stock_id, current_price)

    # 🔹 回應訊息
    message = (
        f"📊 **ETF 資訊 - {stock_id}**\n"
        f"🔹 **當前股價**: {current_price:.2f} 元\n"
        f"💸 **最近一年配息總額**: {total_dividends:.2f} 元 💰\n"
        f"📊 **殖利率**: {dividend_yield:.2f}%\n"
        f"🔹 **配息筆數**: {dividends_count} 筆\n"
    )
    
    await update.message.reply_text(message, parse_mode="Markdown")


# 修改 get_current_stock_price 函數為異步函數
async def get_current_stock_price(stock_id):
    """获取股票当前价格，直接从 API 获取最近5天的数据"""
    try:
        # 获取最近5天的数据
        parameter = {
            "dataset": "TaiwanStockPrice",
            "start_date": (datetime.today() - timedelta(days=5)).strftime('%Y-%m-%d'),
            "token": FINMIND_API_KEY,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(FINMIND_URL, params=parameter) as response:
                if response.status != 200:
                    logger.error(f"API 请求失败，状态码：{response.status}")
                    return None

                data = await response.json()

                # 检查 API 回应是否有数据
                if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                    df_price = pd.DataFrame(data["data"])
                    
                    # 确保日期格式正确
                    df_price['date'] = pd.to_datetime(df_price['date'])
                    
                    # 确保数值字段为数值类型
                    numeric_columns = ['Trading_Volume', 'Trading_money', 'open', 'max', 'min', 'close', 'spread', 'Trading_turnover']
                    for col in numeric_columns:
                        if col in df_price.columns:
                            df_price[col] = pd.to_numeric(df_price[col], errors='coerce')
                    
                    # 获取指定股票的最新收盘价
                    stock_data = df_price[df_price['stock_id'] == stock_id]
                    if not stock_data.empty:
                        latest_price = stock_data.sort_values('date').iloc[-1]['close']
                        logger.info(f"成功获取股票 {stock_id} 的最新价格：{latest_price}")
                        return latest_price
                    else:
                        logger.warning(f"未找到股票 {stock_id} 的价格数据")
                else:
                    logger.warning("API 返回数据为空")

        return None

    except Exception as e:
        logger.error(f"获取股票 {stock_id} 价格时发生错误: {str(e)}")
        return None


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
    # 🔹 過濾該股票的配息資料
    stock_dividends = df_dividend[df_dividend["stock_id"] == stock_id].copy()

    # 確保 date 欄位是 datetime 格式
    stock_dividends["date"] = pd.to_datetime(stock_dividends["date"], errors="coerce")

    if stock_dividends.empty:
        return 0.0, 0.0, 0  # 如果該股票無配息資料，則回傳 0

    # 先按照日期排序（最新的在前）
    stock_dividends = stock_dividends.sort_values(by="date", ascending=False)

    # 取得最近一年的配息
    one_year_ago = datetime.today() - timedelta(days=365)
    today = datetime.today()
    
    # 取得最近一年的配息資料，並確保不重複
    last_year_dividends = stock_dividends[
        (stock_dividends["date"] >= one_year_ago) & 
        (stock_dividends["date"] <= today) &  # 排除未來的配息日期
        (stock_dividends["CashEarningsDistribution"] > 0)  # 只取有現金股利的資料
    ].drop_duplicates(subset=["date"])  # 移除同一天的重複資料
    
    # 確保至少有 1 筆配息資料
    if last_year_dividends.empty:
        return 0.0, 0.0, 0

    # 計算最近一年的 **現金股利總額**
    total_cash_dividends = last_year_dividends["CashEarningsDistribution"].sum()

    # 計算最近一年的 **股票股利總額**
    total_stock_dividends = last_year_dividends["StockEarningsDistribution"].sum()

    # **計算除權息後股價**
    ex_rights_price = max(current_price - total_cash_dividends, 0)  # 確保股價不為負

    # **計算股票股利價值**
    stock_dividend_value = total_stock_dividends * ex_rights_price / 1000

    # **計算總股利價值**
    total_dividend_value = stock_dividend_value + (total_cash_dividends)

    # **計算還原殖利率**
    if current_price > 0:
        restored_dividend_yield = (total_dividend_value / current_price) * 100.00
    else:
        restored_dividend_yield = 0.0

    return total_dividend_value, restored_dividend_yield, len(last_year_dividends)


# 修改 calculate_quarterly_stock_estimates 函數為異步函數
async def calculate_quarterly_stock_estimates(stock_id, start_date="2020-01-01", end_date="2025-12-31"):
    """ 透過 FinMind API 取得 PBR、PER，計算季度 ROE、BVPS、推估股價 """
    parameter = {
        "dataset": "TaiwanStockPER",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": FINMIND_API_KEY,
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(FINMIND_URL, params=parameter) as response:
            data = await response.json()

            if "data" not in data or not isinstance(data["data"], list) or len(data["data"]) == 0:
                return None

            df = pd.DataFrame(data["data"])

            # 確保數據格式
            df["date"] = pd.to_datetime(df["date"])
            df["PBR"] = pd.to_numeric(df["PBR"], errors="coerce")
            df["PER"] = pd.to_numeric(df["PER"], errors="coerce")

            # 計算 ROE (%)
            df["ROE"] = (df["PBR"] / df["PER"]) * 100

            # 依季度取數據
            df["quarter"] = df["date"].dt.to_period("Q")
            
            # 計算季度 PER 統計數據
            df_per_stats = df.groupby("quarter")["PER"].agg([
                ("PER_最高值", "max"),
                ("PER_平均值", "mean"),
                ("PER_最低值", "min")
            ]).reset_index()

            df_quarterly = df.groupby("quarter").last().reset_index()

            # 合併 PER 統計數據
            df_quarterly = df_quarterly.merge(df_per_stats, on="quarter", how="left")

            # 取得目前股價
            current_price = await get_current_stock_price(stock_id)
            if current_price is None:
                return None

            # 計算 BVPS
            df_quarterly["prev_close"] = current_price
            df_quarterly["BVPS"] = df_quarterly["prev_close"] / df_quarterly["PBR"]

            # 計算推估EPS
            df_quarterly["推估EPS"] = (df_quarterly["ROE"] / 100) * df_quarterly["BVPS"]

            # 計算三種股價（高、中、低）
            df_quarterly["高股價"] = df_quarterly["PER_最高值"] * df_quarterly["推估EPS"]
            df_quarterly["正常股價"] = df_quarterly["PER_平均值"] * df_quarterly["推估EPS"]
            df_quarterly["低股價"] = df_quarterly["PER_最低值"] * df_quarterly["推估EPS"]

            return df_quarterly


# 添加獲取台股代號列表的函數
def get_taiwan_stock_list():
    """從 FinMind API 獲取台股代號列表"""
    parameter = {
        "dataset": "TaiwanStockInfo",
        "token": FINMIND_API_KEY,
    }

    try:
        response = requests.get(FINMIND_URL, params=parameter)
        data = response.json()

        if "data" not in data or not isinstance(data["data"], list):
            logger.error("無法從 FinMind API 獲取股票列表")
            return []

        # 轉換為 DataFrame
        df_stocks = pd.DataFrame(data["data"])
        
        # 確保 stock_id 欄位為字串
        df_stocks["stock_id"] = df_stocks["stock_id"].astype(str)
        
        # 過濾掉非上市股票（通常股票代碼長度為 4 位）
        df_stocks = df_stocks[df_stocks["stock_id"].str.len() == 4]
        
        # 過濾掉特殊股票（如權證、期貨等）
        df_stocks = df_stocks[~df_stocks["stock_id"].str.startswith(('0', '9'))]
        
        # 添加日誌記錄
        logger.info(f"從 API 獲取的股票總數：{len(df_stocks)}")
        logger.info(f"股票代碼範圍：{df_stocks['stock_id'].min()} 到 {df_stocks['stock_id'].max()}")
        
        stock_list = df_stocks["stock_id"].tolist()
        logger.info(f"成功獲取 {len(stock_list)} 支上市股票")
        return stock_list
        
    except Exception as e:
        logger.error(f"獲取股票列表時發生錯誤: {str(e)}")
        return []

# 添加斷點續傳相關變量
progress_file = "recommend_v2_progress.json"

def save_progress(stock_list, current_index):
    """保存下載進度"""
    try:
        progress_data = {
            'stock_list': stock_list,
            'current_index': current_index,
            'timestamp': datetime.now().isoformat(),
            'total_stocks': len(stock_list)
        }
        with open(progress_file, 'w') as f:
            json.dump(progress_data, f)
        logger.info(f"已保存進度：當前處理到第 {current_index} 筆，共 {len(stock_list)} 筆")
    except Exception as e:
        logger.error(f"保存下載進度時發生錯誤: {str(e)}")

def load_progress():
    """載入進度"""
    try:
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                data = json.load(f)
                # 檢查進度是否過期（超過 24 小時）
                if datetime.fromisoformat(data['timestamp']) + timedelta(hours=24) < datetime.now():
                    logger.info("下載進度已過期，將重新開始")
                    return None, 0
                
                # 驗證數據完整性
                if 'stock_list' not in data or 'current_index' not in data or 'total_stocks' not in data:
                    logger.error("下載進度檔案格式不正確")
                    return None, 0
                
                logger.info(f"載入下載進度：從第 {data['current_index']} 筆開始，共 {data['total_stocks']} 筆")
                return data['stock_list'], data['current_index']
    except Exception as e:
        logger.error(f"載入下載進度時發生錯誤: {str(e)}")
    return None, 0

# 添加取消命令
async def cancel_recommend(update: Update, context: CallbackContext) -> None:
    """取消正在執行的推薦任務"""
    global is_processing, should_cancel
    
    if not is_processing:
        await update.message.reply_text("目前沒有正在執行的推薦任務")
        return
    
    should_cancel = True
    is_processing = False
    await update.message.reply_text("已發送取消指令，正在等待任務結束...")

# 修改 recommend_v2 函數
async def recommend_v2(update: Update, context: CallbackContext) -> None:
    """推薦股票 v2 版本"""
    try:
        # 直接从 API 获取最近5天的所有股票价格
        parameter = {
            "dataset": "TaiwanStockPrice",
            "start_date": (datetime.today() - timedelta(days=5)).strftime('%Y-%m-%d'),
            "token": FINMIND_API_KEY,
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(FINMIND_URL, params=parameter) as response:
                if response.status != 200:
                    await update.message.reply_text("無法獲取股票數據，請稍後再試")
                    return

                data = await response.json()
                if "data" not in data or not isinstance(data["data"], list) or len(data["data"]) == 0:
                    await update.message.reply_text("API 返回數據為空")
                    return

                # 只保留需要的字段
                price_data = []
                for item in data["data"]:
                    price_data.append({
                        "stock_id": item["stock_id"],
                        "date": item["date"],
                        "close": float(item["close"])
                    })
                
                df_price = pd.DataFrame(price_data)
                # 获取所有股票代码
                stock_list = df_price['stock_id'].unique().tolist()
                logger.info(f"成功獲取 {len(stock_list)} 支股票數據")

        results = []
        
        for stock_id in stock_list:
            try:
                # 获取当前价格（从最近5天的数据中获取最新价格）
                stock_data = df_price[df_price['stock_id'] == stock_id]
                if stock_data.empty:
                    continue
                current_price = stock_data.sort_values('date').iloc[-1]['close']
                
                # 获取估值資料
                df_result = await calculate_quarterly_stock_estimates(stock_id)
                if df_result is None or df_result.empty:
                    continue
                
                # 检查是否有足够的季度資料
                if len(df_result) < 4:
                    continue
                
                # 检查 ROE 是否大于 15
                latest_roe = df_result.iloc[0]["ROE"]
                if latest_roe <= 15:
                    continue
                
                # 计算 ROE 趋势
                roe_values = df_result["ROE"].tolist()
                valid_roe_values = [roe for roe in roe_values if roe > 0]
                if len(valid_roe_values) < 4:
                    continue
                
                roe_trend = all(valid_roe_values[i] >= valid_roe_values[i+1] for i in range(len(valid_roe_values)-1))
                roe_volatility = (max(valid_roe_values) - min(valid_roe_values)) / min(valid_roe_values) * 100
                
                if not roe_trend and roe_volatility > 30:
                    continue
                
                # 计算价值分数
                price_to_low = current_price / df_result.iloc[0]["PER_最低值"]
                current_per = df_result.iloc[0]["PER_最低值"]
                value_score = (price_to_low * 0.7 + (1 / current_per) * 0.3) * 100
                
                results.append({
                    "stock_id": stock_id,
                    "current_price": current_price,
                    "value_score": value_score,
                    "roe": latest_roe,
                    "price_to_low": price_to_low,
                    "current_per": current_per
                })
                
            except Exception as e:
                logger.error(f"處理股票 {stock_id} 時發生錯誤: {str(e)}")
                continue
            
            # 每處理一支股票後暫停一下，避免 API 限制
            await asyncio.sleep(0.5)
        
        # 根據價值分數排序
        results.sort(key=lambda x: x["value_score"], reverse=True)
        
        # 選取前 10 支股票
        top_10 = results[:10]
        
        # 生成推薦訊息
        message = "📊 股票推薦 (v2)\n\n"
        for i, stock in enumerate(top_10, 1):
            message += f"{i}. {stock['stock_id']}\n"
            message += f"   現價: {stock['current_price']:.2f}\n"
            message += f"   價值分數: {stock['value_score']:.2f}\n"
            message += f"   ROE: {stock['roe']:.2f}%\n"
            message += f"   股價/低點: {stock['price_to_low']:.2f}\n"
            message += f"   本益比: {stock['current_per']:.2f}\n\n"
        
        await update.message.reply_text(message)
        
    except Exception as e:
        logger.error(f"推薦股票時發生錯誤: {str(e)}")
        await update.message.reply_text("處理過程中發生錯誤，請稍後再試")

def main():
    global app
    load_dotenv()  # 載入 .env 變數
    
    # 讀取 Heroku 環境變數
    BOT_TOKEN = os.getenv("BOT_TOKEN")

    if not BOT_TOKEN:
        raise ValueError("未找到 BOT_TOKEN，請在 Heroku 環境變數設定 BOT_TOKEN")
    
    # 設置信號處理
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        app = Application.builder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("recommend_v2", recommend_v2))
        app.add_handler(CommandHandler("cancel_recommend", cancel_recommend))  # 添加取消命令
        app.add_handler(CommandHandler("etf", etf))
        app.add_handler(CommandHandler("stock_estimate", stock_estimate))

        logger.info("Bot 已啟動並開始運行...")
        app.run_polling()
        
    except Exception as e:
        logger.error(f"運行時發生錯誤: {str(e)}")
        if app:
            app.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
