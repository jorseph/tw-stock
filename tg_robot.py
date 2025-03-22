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

# 在文件開頭添加緩存相關變量
CACHE_FILE = "stock_cache.json"  # 暫存檔案名稱
CACHE_DURATION = timedelta(hours=24)  # 緩存有效期為 24 小時

# 全局緩存變量
price_cache = {}  # 股價緩存

def save_cache():
    """保存暫存資料"""
    try:
        logger.info("開始保存緩存數據...")
        cache_data = {
            'price_cache': price_cache
        }
        
        logger.info(f"股價緩存數量：{len(price_cache)}")
        
        logger.info(f"開始寫入緩存文件：{CACHE_FILE}")
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_data, f)
        logger.info("緩存數據保存完成")
    except Exception as e:
        logger.error(f"保存暫存時發生錯誤: {str(e)}")
        logger.error(f"錯誤詳情: {type(e).__name__}")
        import traceback
        logger.error(f"錯誤堆疊: {traceback.format_exc()}")

def load_cache():
    """載入暫存資料"""
    try:
        if os.path.exists(CACHE_FILE):
            logger.info(f"開始載入緩存文件：{CACHE_FILE}")
            with open(CACHE_FILE, 'r') as f:
                cache_data = json.load(f)
                
                # 檢查 price_cache
                if 'price_cache' in cache_data:
                    logger.info(f"成功載入股價緩存，共 {len(cache_data['price_cache'])} 支股票")
                    logger.info(f"前 5 支股票的股價：{dict(list(cache_data['price_cache'].items())[:5])}")
                else:
                    logger.error("緩存文件中沒有 price_cache 數據")
                
                logger.info("緩存載入完成")
                return cache_data.get('price_cache', {})
        else:
            logger.warning(f"緩存文件 {CACHE_FILE} 不存在")
    except Exception as e:
        logger.error(f"載入暫存時發生錯誤: {str(e)}")
        logger.error(f"錯誤詳情: {type(e).__name__}")
        import traceback
        logger.error(f"錯誤堆疊: {traceback.format_exc()}")
    return {}

# 載入暫存資料
price_cache = load_cache()

def get_cached_price(stock_id):
    """從緩存獲取股價"""
    global price_cache
    
    try:
        # 確保緩存已載入
        if not price_cache:
            logger.info("股價緩存為空，嘗試重新載入")
            price_cache = load_cache()
            
        # 檢查股票是否在緩存中
        if stock_id in price_cache:
            logger.info(f"成功從緩存獲取股票 {stock_id} 的股價：{price_cache[stock_id]}")
            return price_cache[stock_id]
        else:
            logger.warning(f"股票 {stock_id} 不在股價緩存中")
            logger.info(f"當前緩存中的股票數量：{len(price_cache)}")
            logger.info(f"緩存中的股票列表：{list(price_cache.keys())[:10]}...")  # 只顯示前10支股票
            return None
    except Exception as e:
        logger.error(f"獲取緩存股價時發生錯誤: {str(e)}")
        return None

def set_cached_price(stock_id, price):
    """設置股價緩存"""
    price_cache[stock_id] = price
    save_cache()  # 保存到檔案

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
    # 先檢查緩存
    cached_price = get_cached_price(stock_id)
    if cached_price is not None:
        return cached_price

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

        async with aiohttp.ClientSession() as session:
            async with session.get(FINMIND_URL, params=parameter) as response:
                data = await response.json()

                # 檢查 API 回應是否有數據
                if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                    df_price = pd.DataFrame(data["data"])
                    latest_price = df_price.sort_values(by="date", ascending=False).iloc[0]["close"]
                    # 設置緩存
                    set_cached_price(stock_id, latest_price)
                    return latest_price

                # 如果沒有數據，向前推一天
                check_date -= timedelta(days=1)

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
        
        return df_stocks["stock_id"].tolist()
        
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
    global is_processing, should_cancel
    
    # 檢查是否已經在執行
    if is_processing:
        await update.message.reply_text("⚠️ 已經有一個推薦任務正在執行中，請等待完成或使用 /cancel_recommend 取消")
        return
    
    # 重置取消標記
    should_cancel = False
    
    # 設定預設推薦數量為 5
    count = 5
    
    if context.args:
        try:
            count = int(context.args[0])
            if count <= 0:
                await update.message.reply_text("請輸入大於 0 的數量，例如：/recommend_v2 5")
                return
            if count > 10:
                await update.message.reply_text("最多只能推薦 10 檔股票，請輸入小於等於 10 的數字")
                return
        except ValueError:
            await update.message.reply_text("請輸入有效的數字，例如：/recommend_v2 5")
            return

    logger.info(f"開始執行 recommend_v2，推薦數量：{count}")
    
    # 設置執行狀態
    is_processing = True
    current_task = asyncio.current_task()
    
    try:
        # 嘗試載入上次的下載進度
        stock_list, download_index = load_progress()
        
        # 如果沒有下載進度或進度已過期，重新獲取股票列表
        if not stock_list:
            stock_list = get_taiwan_stock_list()
            if not stock_list:
                await update.message.reply_text("⚠️ 無法獲取股票列表，請稍後再試")
                return
            download_index = 0
            save_progress(stock_list, download_index)
            logger.info(f"重新開始下載：總股票數量 {len(stock_list)}")
        
        logger.info(f"總股票數量：{len(stock_list)}，從第 {download_index} 筆開始下載")
        
        # 分批下載股票數據
        batch_size = 100  # 每批處理 100 支股票
        for i in range(download_index, len(stock_list), batch_size):
            # 檢查是否被取消
            if should_cancel:
                logger.info("收到取消指令，正在結束任務...")
                await update.message.reply_text("⚠️ 推薦任務已被取消")
                return
                
            batch_stocks = stock_list[i:i + batch_size]
            logger.info(f"正在下載第 {i+1} 到 {min(i+batch_size, len(stock_list))} 支股票")
            
            try:
                # 檢查哪些股票需要獲取股價
                uncached_stocks = [stock_id for stock_id in batch_stocks if get_cached_price(stock_id) is None]
                logger.info(f"需要獲取股價的股票數量：{len(uncached_stocks)}")
                logger.info(f"需要獲取股價的股票列表：{uncached_stocks[:5]}...")  # 只顯示前5支股票
                
                if uncached_stocks:
                    # 只對未緩存的股票進行 API 請求
                    tasks = [get_current_stock_price(stock_id) for stock_id in uncached_stocks]
                    prices = await asyncio.gather(*tasks)
                    
                    for stock_id, price in zip(uncached_stocks, prices):
                        if price is not None:
                            set_cached_price(stock_id, price)
                            logger.info(f"成功獲取並緩存股票 {stock_id} 的股價：{price}")
                
                # 更新下載進度
                next_index = i + batch_size
                if next_index >= len(stock_list):
                    next_index = len(stock_list)
                save_progress(stock_list, next_index)
                logger.info(f"已更新下載進度到第 {next_index} 筆")
                
                # 定期保存緩存
                save_cache()
                logger.info("已保存緩存數據")
                
            except Exception as e:
                logger.error(f"下載批次時發生錯誤: {str(e)}")
                logger.error(f"錯誤詳情: {type(e).__name__}")
                import traceback
                logger.error(f"錯誤堆疊: {traceback.format_exc()}")
                # 保存當前下載進度
                save_progress(stock_list, i)
                await update.message.reply_text(f"⚠️ 下載過程中發生錯誤，已保存進度，下次將從第 {i+1} 支股票繼續下載")
                return
        
        # 開始評估股票
        logger.info("開始評估股票...")
        
        # 儲存所有股票的評估結果
        stock_evaluations = []
        processed_stocks = set()  # 用於追蹤已處理的股票
        
        # 分批評估股票
        for i in range(0, len(stock_list), batch_size):
            # 檢查是否被取消
            if should_cancel:
                logger.info("收到取消指令，正在結束任務...")
                await update.message.reply_text("⚠️ 推薦任務已被取消")
                return
                
            batch_stocks = stock_list[i:i + batch_size]
            logger.info(f"正在評估第 {i+1} 到 {min(i+batch_size, len(stock_list))} 支股票")
            
            try:
                # 處理評估結果
                for stock_id in batch_stocks:
                    try:
                        # 檢查是否已經處理過這支股票
                        if stock_id in processed_stocks:
                            logger.info(f"股票 {stock_id} 已經處理過，跳過")
                            continue
                            
                        # 從緩存獲取股價
                        current_price = get_cached_price(stock_id)
                        
                        if current_price is None:
                            logger.info(f"股票 {stock_id} 沒有股價數據")
                            continue
                            
                        # 直接計算估值數據
                        df_result = await calculate_quarterly_stock_estimates(stock_id)
                        if df_result is None:
                            logger.info(f"股票 {stock_id} 無法計算估值數據")
                            continue
                            
                        # 輸出數據的內容
                        logger.info(f"股票 {stock_id} 的數據：")
                        logger.info(f"股價：{current_price}")
                        logger.info(f"估值數據：\n{df_result}")
                            
                        # 取最近 4 季的資料
                        last_4q = df_result.tail(4)
                        if len(last_4q) < 4:
                            logger.info(f"股票 {stock_id} 的季度數據不足 4 季，只有 {len(last_4q)} 季")
                            continue
                            
                        # 取最近一季的資料
                        latest_data = last_4q.iloc[-1]
                        logger.info(f"股票 {stock_id} 最近一季數據：\n{latest_data}")
                        
                        # 檢查 ROE 趨勢
                        roe_values = last_4q["ROE"].values
                        valid_roe_values = [x for x in roe_values if not pd.isna(x) and np.isfinite(x)]
                        logger.info(f"股票 {stock_id} 的 ROE 值：{roe_values}")
                        logger.info(f"股票 {stock_id} 的有效 ROE 值：{valid_roe_values}")
                        
                        if len(valid_roe_values) < 4:
                            logger.info(f"股票 {stock_id} 的有效 ROE 數據不足 4 季，只有 {len(valid_roe_values)} 季")
                            continue
                            
                        # 計算 ROE 趨勢
                        roe_trend = np.diff(valid_roe_values)
                        roe_increasing = all(x > 0 for x in roe_trend)
                        roe_decline_ratio = (max(valid_roe_values) - min(valid_roe_values)) / max(valid_roe_values) if max(valid_roe_values) > 0 else float('inf')
                        logger.info(f"股票 {stock_id} 的 ROE 趨勢：{roe_trend}")
                        logger.info(f"股票 {stock_id} 的 ROE 是否上升：{roe_increasing}")
                        logger.info(f"股票 {stock_id} 的 ROE 波動率：{roe_decline_ratio}")
                        
                        # 檢查 ROE 是否大於 15
                        if latest_data["ROE"] <= 15:
                            logger.info(f"股票 {stock_id} 的 ROE ({latest_data['ROE']}) 小於等於 15，不符合條件")
                            continue
                            
                        if not roe_increasing and roe_decline_ratio > 0.3:
                            logger.info(f"股票 {stock_id} 的 ROE 趨勢不符合條件")
                            continue
                            
                        price_to_low = current_price / latest_data["低股價"] if latest_data["低股價"] > 0 else float('inf')
                        price_to_normal = current_price / latest_data["正常股價"] if latest_data["正常股價"] > 0 else float('inf')
                        logger.info(f"股票 {stock_id} 的價格比率：")
                        logger.info(f"price_to_low: {price_to_low}")
                        logger.info(f"price_to_normal: {price_to_normal}")
                        
                        value_score = 0
                        
                        if current_price < latest_data["低股價"]:
                            value_score += 3
                        elif current_price < latest_data["正常股價"]:
                            value_score += 2
                        elif current_price < latest_data["高股價"]:
                            value_score += 1
                            
                        if latest_data["ROE"] > 15:
                            value_score += 3
                        elif latest_data["ROE"] > 10:
                            value_score += 2
                        elif latest_data["ROE"] > 8:
                            value_score += 1
                            
                        current_per = current_price / latest_data["推估EPS"] if latest_data["推估EPS"] > 0 else float('inf')
                        if current_per < latest_data["PER_最低值"]:
                            value_score += 3
                        elif current_per < latest_data["PER_平均值"]:
                            value_score += 2
                        elif current_per < latest_data["PER_最高值"]:
                            value_score += 1
                            
                        if roe_increasing:
                            value_score += 3
                        elif roe_decline_ratio < 0.1:
                            value_score += 2
                        elif roe_decline_ratio < 0.2:
                            value_score += 1
                        
                        logger.info(f"股票 {stock_id} 的評分計算：")
                        logger.info(f"value_score: {value_score}")
                        logger.info(f"current_per: {current_per}")
                        
                        # 添加到評估結果列表
                        stock_evaluations.append({
                            "stock_id": stock_id,
                            "目前股價": current_price,
                            "ROE": latest_data["ROE"],
                            "推估EPS": latest_data["推估EPS"],
                            "低股價": latest_data["低股價"],
                            "正常股價": latest_data["正常股價"],
                            "高股價": latest_data["高股價"],
                            "value_score": value_score,
                            "price_to_low": price_to_low,
                            "price_to_normal": price_to_normal,
                            "roe_decline_ratio": roe_decline_ratio * 100,
                            "roe_trend": "上升" if roe_increasing else "下降"
                        })
                        
                        # 記錄已處理的股票
                        processed_stocks.add(stock_id)
                        logger.info(f"成功評估股票 {stock_id}，分數：{value_score}")
                        
                    except Exception as e:
                        logger.error(f"處理股票 {stock_id} 時發生錯誤: {str(e)}")
                        continue
                
            except Exception as e:
                logger.error(f"評估批次時發生錯誤: {str(e)}")
                await update.message.reply_text("⚠️ 評估過程中發生錯誤")
                return
        
        logger.info(f"成功評估的股票數量：{len(stock_evaluations)}")
        logger.info(f"已處理的股票數量：{len(processed_stocks)}")
        
        if not stock_evaluations:
            await update.message.reply_text("⚠️ 沒有找到符合條件的股票")
            return
        
        # 確保沒有重複的股票
        unique_stocks = {stock["stock_id"]: stock for stock in stock_evaluations}.values()
        sorted_stocks = sorted(unique_stocks, 
                             key=lambda x: (-x["value_score"], x["price_to_normal"]))[:count]
        
        logger.info(f"最終推薦的股票數量：{len(sorted_stocks)}")
        logger.info(f"推薦的股票列表：{[stock['stock_id'] for stock in sorted_stocks]}")
        
        message = f"📊 **推薦股票 V2 版本（前 {count} 名）**\n\n"
        for stock in sorted_stocks:
            message += (
                f"🔹 **{stock['stock_id']}**\n"
                f"   💰 **目前股價**: {stock['目前股價']:.2f} 元\n"
                f"   📊 **ROE**: {stock['ROE']:.2f}%\n"
                f"   📊 **ROE趨勢**: {stock['roe_trend']}\n"
                f"   📊 **ROE波動**: {stock['roe_decline_ratio']:.2f}%\n"
                f"   💵 **推估EPS**: {stock['推估EPS']:.2f}\n"
                f"   📉 **低股價**: {stock['低股價']:.2f} 元\n"
                f"   📊 **正常股價**: {stock['正常股價']:.2f} 元\n"
                f"   📈 **高股價**: {stock['高股價']:.2f} 元\n"
                f"   ⭐ **投資價值分數**: {stock['value_score']}\n"
                "--------------------\n"
            )
        
        await update.message.reply_text(message, parse_mode="Markdown")
        
    finally:
        # 重置執行狀態
        is_processing = False
        should_cancel = False
        current_task = None

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
