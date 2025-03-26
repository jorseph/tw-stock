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
from typing import Dict, List, Optional
import pickle

# 設定日誌
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    encoding='utf-8'  # 添加 UTF-8 編碼
)
logger = logging.getLogger(__name__)

# 創建一個全局的 application 變量
app = None

# 添加全局變量來追蹤執行狀態
is_processing = False
current_task = None
should_cancel = False

# 緩存相關常量
CACHE_FILE = "stock_data_cache.pkl"
CACHE_EXPIRY_DAYS = 7  # 改為 7 天，因為基本面數據變化較慢
BATCH_SIZE = 100  # 增加批次大小
DELAY_BETWEEN_BATCHES = 1  # 減少批次間延遲到 30 秒
MAX_CONCURRENT_REQUESTS = 10  # 增加並發請求數

# 緩存數據結構
class StockDataCache:
    def __init__(self):
        self.data: Dict[str, Dict] = {}
        self.last_update: Dict[str, datetime] = {}
    
    def is_valid(self, stock_id: str) -> bool:
        if stock_id not in self.last_update:
            return False
        return (datetime.now() - self.last_update[stock_id]).days < CACHE_EXPIRY_DAYS
    
    def get(self, stock_id: str) -> Optional[Dict]:
        if self.is_valid(stock_id):
            return self.data.get(stock_id)
        return None
    
    def set(self, stock_id: str, data: Dict):
        self.data[stock_id] = data
        self.last_update[stock_id] = datetime.now()
    
    def save(self):
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(self, f)
    
    @classmethod
    def load(cls) -> 'StockDataCache':
        try:
            with open(CACHE_FILE, 'rb') as f:
                return pickle.load(f)
        except (FileNotFoundError, pickle.PickleError):
            return cls()

# 全局緩存對象
stock_cache = StockDataCache.load()

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
async def calculate_quarterly_stock_estimates(stock_id, start_date="2020-01-01", end_date=None):
    """ 從 CSV 文件讀取數據，計算季度 ROE、BVPS、推估股價 """
    try:
        # 讀取 CSV 文件
        csv_file = "stock_roe_data.csv"
        if not os.path.exists(csv_file):
            logger.error(f"找不到 {csv_file} 文件")
            return None

        # 讀取數據並過濾指定股票
        df = pd.read_csv(csv_file)
        df['stock_id'] = df['stock_id'].astype(str)
        df = df[df['stock_id'] == stock_id]

        if df.empty:
            logger.warning(f"股票 {stock_id} 在 CSV 中沒有數據")
            return None

        # 確保日期格式正確
        df["date"] = pd.to_datetime(df["date"])
        
        # 確保數值欄位為數值類型
        numeric_columns = ["PER", "PBR"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 過濾無效的 PER 和 PBR 數據
        df = df[
            (df['PER'] > 0) & (df['PER'] < 100) &  # 合理的 PER 範圍
            (df['PBR'] > 0) & (df['PBR'] < 10)     # 合理的 PBR 範圍
        ]

        if df.empty:
            logger.warning(f"股票 {stock_id} 沒有有效的 PER 和 PBR 數據")
            return None

        # 計算 ROE (%)
        df["ROE"] = np.where(
            (df['PER'] != 0) & (df['PER'].notna()) & (df['PBR'].notna()),
            (df['PBR'] / df['PER']) * 100,
            np.nan
        )

        # 依季度取數據
        df["quarter"] = df["date"].dt.to_period("Q")
        
        # 計算季度 PER 統計數據（使用百分位數避免極端值）
        df_per_stats = df.groupby("quarter").agg(
            PER_最高值=("PER", lambda x: np.percentile(x, 95)),  # 95th percentile
            PER_平均值=("PER", "mean"),
            PER_最低值=("PER", lambda x: np.percentile(x, 5))    # 5th percentile
        ).reset_index()

        # 計算每季度的平均值
        df_quarterly = df.groupby("quarter").agg(
            date=("date", "last"),
            PER=("PER", "mean"),
            PBR=("PBR", "median"),
            ROE=("ROE", "median")
        ).reset_index()

        # 合併 PER 統計數據
        df_quarterly = df_quarterly.merge(df_per_stats, on="quarter", how="left")

        # 取得目前股價
        current_price = await get_current_stock_price(stock_id)
        if current_price is None or current_price <= 0:
            logger.warning(f"股票 {stock_id} 無法獲取有效的當前股價")
            return None

        # 計算 BVPS
        df_quarterly["prev_close"] = current_price
        df_quarterly["BVPS"] = df_quarterly["prev_close"] / df_quarterly["PBR"]

        # 計算推估EPS
        df_quarterly["推估EPS"] = df_quarterly["BVPS"] * (df_quarterly["ROE"] / 100)

        # 計算三種股價（使用 PER 的百分位數）
        df_quarterly["高股價"] = df_quarterly["PER_最高值"] * df_quarterly["推估EPS"]
        df_quarterly["正常股價"] = df_quarterly["PER_平均值"] * df_quarterly["推估EPS"]
        df_quarterly["低股價"] = df_quarterly["PER_最低值"] * df_quarterly["推估EPS"]

        # 按日期排序（最新的在前）
        df_quarterly = df_quarterly.sort_values("date", ascending=False)

        # 移除無效的估值
        df_quarterly = df_quarterly[
            df_quarterly[["ROE", "BVPS", "推估EPS", "高股價", "正常股價", "低股價"]].notna().all(axis=1)
        ]

        if df_quarterly.empty:
            logger.warning(f"股票 {stock_id} 無有效的季度數據")
            return None

        # 檢查是否有足夠的季度數據
        if len(df_quarterly) < 4:
            logger.warning(f"股票 {stock_id} 的季度數據不足 4 季")
            return None

        # 檢查最新數據是否在最近一年內
        latest_date = df_quarterly.iloc[0]["date"]
        one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
        
        if latest_date < one_year_ago:
            logger.warning(f"股票 {stock_id} 的最新數據過期（{latest_date.strftime('%Y-%m-%d')}）")
            return None

        # 只返回最近 4 季的數據
        df_quarterly = df_quarterly.head(4)

        return df_quarterly

    except Exception as e:
        logger.error(f"處理股票 {stock_id} 數據時發生錯誤: {str(e)}")
        return None

# 添加獲取台股代號列表的函數
def get_taiwan_stock_list():
    """從  FinMind API 獲取台股代號列表"""
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

async def recommend_v2(update: Update, context: CallbackContext) -> None:
    """推薦股票 v2 版本"""
    global is_processing, should_cancel
    
    if is_processing:
        logger.warning("已有推薦任務正在執行中")
        await update.message.reply_text("已有推薦任務正在執行中，請稍後再試")
        return
    
    try:
        is_processing = True
        should_cancel = False
        logger.info("開始執行股票推薦任務")
        
        # 讀取 ROE 數據 CSV 文件
        csv_file = "stock_roe_data.csv"
        if not os.path.exists(csv_file):
            logger.error(f"找不到 ROE 數據文件：{csv_file}")
            await update.message.reply_text("找不到 ROE 數據文件，請先執行 /get_roe_data 命令")
            return
            
        try:
            # 讀取 ROE 數據
            logger.info("開始讀取 ROE 數據文件")
            df_roe = pd.read_csv(csv_file)
            df_roe['stock_id'] = df_roe['stock_id'].astype(str)
            df_roe['date'] = pd.to_datetime(df_roe['date'])
            logger.info(f"成功讀取 ROE 數據，共 {len(df_roe)} 筆記錄")

            # 讀取股價數據
            if not os.path.exists(STOCK_PRICE_FILE):
                logger.error(f"找不到股價數據文件：{STOCK_PRICE_FILE}")
                await update.message.reply_text("找不到股價數據文件，請先執行 /sync_stock_prices 命令")
                return

            logger.info("開始讀取股價數據文件")
            with open(STOCK_PRICE_FILE, 'r', encoding='utf-8') as f:
                stock_prices = json.load(f)
            logger.info(f"成功讀取股價數據，共 {len(stock_prices)} 支股票")
            
        except Exception as e:
            logger.error(f"讀取數據文件時發生錯誤: {str(e)}", exc_info=True)
            await update.message.reply_text("讀取數據文件時發生錯誤，請稍後再試")
            return

        # 獲取所有股票代碼
        stock_list = df_roe['stock_id'].unique().tolist()
        logger.info(f"開始處理股票，總共 {len(stock_list)} 支股票")

        # 處理每支股票
        all_results = []
        total_stocks = len(stock_list)
        processed_count = 0
        filtered_count = 0
        no_quarter_data_count = 0
        
        for stock_id in stock_list:
            if should_cancel:
                logger.info("收到取消指令，停止處理")
                await update.message.reply_text("任務已取消")
                break
                
            try:
                # 獲取該股票的所有數據
                stock_data = df_roe[df_roe['stock_id'] == stock_id].copy()
                if stock_data.empty:
                    logger.debug(f"股票 {stock_id} 無 ROE 數據，跳過")
                    continue

                # 確保數據按日期排序（最新的在前）
                stock_data = stock_data.sort_values('date', ascending=False)
                
                # 檢查是否有近四季數據
                if len(stock_data) < 4:
                    logger.debug(f"股票 {stock_id} 的季度數據不足 4 季，跳過")
                    no_quarter_data_count += 1
                    continue

                # 取最近 4 季數據
                recent_data = stock_data.head(4).copy()
                
                # 使用 .loc 計算 ROE，並添加數值檢查
                recent_data.loc[:, 'ROE'] = np.where(
                    (recent_data['PER'] != 0) & (recent_data['PER'].notna()) & (recent_data['PBR'].notna()),
                    (recent_data['PBR'] / recent_data['PER']) * 100,
                    np.nan
                )
                
                # 檢查 ROE 是否有效
                if recent_data['ROE'].isna().all():
                    logger.debug(f"股票 {stock_id} 的 ROE 計算結果全為無效值，跳過")
                    continue
                
                # 檢查 ROE 是否大於 15
                latest_roe = recent_data['ROE'].iloc[0]
                if pd.isna(latest_roe) or latest_roe <= 15:
                    logger.debug(f"股票 {stock_id} 的 ROE ({latest_roe if not pd.isna(latest_roe) else 'NA'}%) 無效或低於 15%，跳過")
                    continue

                # 檢查 PER 是否為 0
                latest_per = recent_data['PER'].iloc[0]
                if pd.isna(latest_per) or latest_per == 0:
                    logger.debug(f"股票 {stock_id} 的 PER ({latest_per if not pd.isna(latest_per) else 'NA'}) 無效或為 0，跳過")
                    continue

                # 檢查最近四季是否有任何一季的 PER 或 ROE 為 0
                if (recent_data['PER'] == 0).any() or (recent_data['ROE'] == 0).any():
                    logger.debug(f"股票 {stock_id} 的最近四季中有 PER 或 ROE 為 0 的數據，跳過")
                    continue

                # 計算 ROE 趨勢
                roe_values = recent_data['ROE'].dropna().tolist()
                valid_roe_values = [roe for roe in roe_values if not pd.isna(roe) and roe > 0]
                if len(valid_roe_values) < 4:
                    logger.debug(f"股票 {stock_id} 的有效 ROE 數據不足 4 季，跳過")
                    continue
                
                # 檢查 ROE 是否穩定向上升
                roe_trend = all(valid_roe_values[i] <= valid_roe_values[i+1] for i in range(len(valid_roe_values)-1))
                roe_volatility = (max(valid_roe_values) - min(valid_roe_values)) / min(valid_roe_values) * 100 if min(valid_roe_values) > 0 else float('inf')
                
                # 要求 ROE 穩定向上升且波動率不超過 20%
                if not roe_trend or roe_volatility > 20:
                    logger.debug(f"股票 {stock_id} 的 ROE 趨勢不符合要求（趨勢：{'上升' if roe_trend else '下降'}, 波動率：{roe_volatility:.2f}%），跳過")
                    continue

                # 獲取當前股價
                current_price = stock_prices.get(stock_id)
                if current_price is None or current_price <= 0:
                    logger.debug(f"股票 {stock_id} 無當前股價數據或股價無效，跳過")
                    continue

                # 計算 BVPS 和推估 EPS
                latest_data = recent_data.iloc[0]
                if pd.isna(latest_data['PBR']) or latest_data['PBR'] <= 0:
                    logger.debug(f"股票 {stock_id} 的 PBR 無效，跳過")
                    continue
                
                bvps = current_price / latest_data['PBR']
                estimated_eps = (latest_roe / 100) * bvps if not pd.isna(latest_roe) else 0

                # 計算三種股價（添加數值檢查）
                if pd.isna(latest_data['PER']) or latest_data['PER'] <= 0 or pd.isna(estimated_eps) or estimated_eps <= 0:
                    logger.debug(f"股票 {stock_id} 的 PER 或 EPS 無效，跳過")
                    continue

                low_price = latest_data['PER'] * 0.8 * estimated_eps
                normal_price = latest_data['PER'] * estimated_eps
                high_price = latest_data['PER'] * 1.2 * estimated_eps

                # 計算價值分數（添加數值檢查）
                if low_price <= 0:
                    logger.debug(f"股票 {stock_id} 的低估價格計算結果無效，跳過")
                    continue

                price_to_low = current_price / low_price
                value_score = (price_to_low * 0.7 + (1 / latest_data['PER']) * 0.3) * 100
                
                result = {
                    "stock_id": stock_id,
                    "current_price": current_price,
                    "value_score": value_score,
                    "roe": latest_roe,
                    "price_to_low": price_to_low,
                    "current_per": latest_data['PER'],
                    "roe_trend": roe_trend,
                    "roe_volatility": roe_volatility,
                    "低股價": low_price,
                    "正常股價": normal_price,
                    "高股價": high_price,
                    "推估EPS": estimated_eps
                }
                
                all_results.append(result)
                filtered_count += 1
                logger.debug(f"股票 {stock_id} 符合篩選條件（ROE: {latest_roe:.2f}%, 價值分數: {value_score:.2f}）")

                processed_count += 1
                # 每處理 100 支股票發送一次進度更新
                if processed_count % 100 == 0:
                    progress = (processed_count / total_stocks) * 100
                    logger.info(f"處理進度：{progress:.1f}%，已處理 {processed_count} 支股票，符合條件 {filtered_count} 支，無四季資料 {no_quarter_data_count} 支")
                    await update.message.reply_text(f"處理進度：{progress:.1f}% ({processed_count}/{total_stocks})\n符合條件：{filtered_count} 支\n無四季資料：{no_quarter_data_count} 支")
                
            except Exception as e:
                logger.error(f"處理股票 {stock_id} 時發生錯誤: {str(e)}", exc_info=True)
                continue
        
        if not all_results:
            logger.warning("沒有找到符合條件的股票")
            await update.message.reply_text("沒有找到符合條件的股票")
            return

        logger.info(f"篩選完成，共有 {len(all_results)} 支股票符合條件，{no_quarter_data_count} 支股票無四季資料")

        # 根據價值分數排序
        all_results.sort(key=lambda x: x["value_score"], reverse=True)
        
        # 選取前 10 支股票
        top_10 = all_results[:10]
        logger.info("前 10 名股票（依價值分數排序）：" + ", ".join([f"{stock['stock_id']}({stock['value_score']:.2f})" for stock in top_10]))
        
        # 生成推薦訊息
        message = "📊 股票推薦 (v2)\n\n"
        message += f"🔹 處理統計：\n"
        message += f"- 總股票數：{total_stocks} 支\n"
        message += f"- 無四季資料：{no_quarter_data_count} 支\n"
        message += f"- 符合條件：{filtered_count} 支\n\n"
        message += "🔹 根據價值分數排序：\n"
        for i, stock in enumerate(top_10, 1):
            message += f"{i}. {stock['stock_id']}\n"
            message += f"   當前股價: {stock['current_price']:.2f}\n"
            message += f"   價值分數: {stock['value_score']:.2f}\n"
            message += f"   ROE: {stock['roe']:.2f}%\n"
            message += f"   推估EPS: {stock['推估EPS']:.2f}\n"
            message += f"   低股價: {stock['低股價']:.2f}\n"
            message += f"   正常股價: {stock['正常股價']:.2f}\n"
            message += f"   高股價: {stock['高股價']:.2f}\n"
            message += f"   本益比: {stock['current_per']:.2f}\n"
            message += f"   ROE趨勢: {'上升' if stock['roe_trend'] else '下降'}\n"
            message += f"   ROE波動率: {stock['roe_volatility']:.2f}%\n\n"
        
        logger.info("完成推薦訊息生成，準備發送")
        await update.message.reply_text(message)
        logger.info("推薦訊息已發送")
        
    except Exception as e:
        logger.error(f"推薦股票時發生錯誤: {str(e)}", exc_info=True)
        await update.message.reply_text("處理過程中發生錯誤，請稍後再試")
    finally:
        is_processing = False
        logger.info("推薦任務結束")

# 添加新的常量
ROE_DATA_FILE = "stock_roe_data.json"

async def get_stock_roe_data(update: Update, context: CallbackContext) -> None:
    """获取所有台股的 ROE 数据并保存為 CSV"""
    try:
        # 获取所有台股代码
        stock_list = get_taiwan_stock_list()
        if not stock_list:
            await update.message.reply_text("無法獲取台股列表，請稍後再試")
            return
            
        logger.info(f"成功獲取 {len(stock_list)} 支台股代碼")

        # 创建 CSV 文件
        csv_file = "stock_roe_data.csv"
        no_data_file = "no_data_stocks.json"  # 记录没有数据的股票
        processed_count = 0
        existing_stocks = set()
        no_data_stocks = set()

        # 读取没有数据的股票列表
        if os.path.exists(no_data_file):
            try:
                with open(no_data_file, 'r', encoding='utf-8') as f:
                    no_data_stocks = set(json.load(f))
                    logger.info(f"讀取到 {len(no_data_stocks)} 支沒有數據的股票")
            except Exception as e:
                logger.error(f"讀取無數據股票列表時發生錯誤: {str(e)}")
                no_data_stocks = set()

        # 检查现有的 CSV 文件
        if os.path.exists(csv_file):
            try:
                df_existing = pd.read_csv(csv_file)
                # 确保 stock_id 为字符串类型
                df_existing['stock_id'] = df_existing['stock_id'].astype(str)
                existing_stocks = set(df_existing['stock_id'].unique())
                logger.info(f"現有 CSV 文件中已有 {len(existing_stocks)} 支股票的數據")
                logger.info(f"CSV 中的股票示例：{list(existing_stocks)[:5]}")
            except Exception as e:
                logger.error(f"讀取現有 CSV 文件時發生錯誤: {str(e)}")
                existing_stocks = set()

        logger.info(f"本次將處理前 {len(stock_list)} 支股票")

        # 获取需要处理的股票列表（排除已有数据的股票）
        missing_stocks = [stock_id for stock_id in stock_list 
                        if stock_id not in existing_stocks]
        logger.info(f"其中 {len(missing_stocks)} 支股票需要處理")

        if not missing_stocks:
            await update.message.reply_text("所有股票數據都已是最新")
            return

        # 处理缺失的股票
        new_no_data_stocks = set()  # 记录本次执行中发现没有数据的股票
        for stock_id in missing_stocks:
            try:
                logger.info(f"正在查詢股票 {stock_id} 的 ROE 數據...")
                # 设置 API 参数
                parameter = {
                    "dataset": "TaiwanStockPER",
                    "data_id": stock_id,
                    "start_date": "2020-01-01",
                    "end_date": datetime.now().strftime('%Y-%m-%d'),
                    "token": FINMIND_API_KEY,
                }

                # 获取 ROE 数据
                async with aiohttp.ClientSession() as session:
                    async with session.get(FINMIND_URL, params=parameter) as response:
                        if response.status != 200:
                            logger.error(f"API 請求失敗，股票 {stock_id}，狀態碼：{response.status}")
                            new_no_data_stocks.add(stock_id)
                            continue

                        data = await response.json()
                        if "data" not in data or not isinstance(data["data"], list) or len(data["data"]) == 0:
                            logger.warning(f"股票 {stock_id} 沒有數據")
                            new_no_data_stocks.add(stock_id)
                            continue

                        logger.info(f"成功獲取股票 {stock_id} 的 ROE 數據")

                        # 转换数据为 DataFrame
                        df = pd.DataFrame(data["data"])
                        
                        # 确保日期格式正确
                        df["date"] = pd.to_datetime(df["date"])
                        
                        # 确保数值字段为数值类型
                        numeric_columns = ["PER", "PBR", "ROE"]
                        for col in numeric_columns:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors="coerce")

                        # 添加股票代码列
                        df["stock_id"] = stock_id

                        # 将数据写入 CSV
                        if processed_count == 0 and not os.path.exists(csv_file):
                            # 第一次写入，创建文件并写入表头
                            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                        else:
                            # 后续写入，追加数据（不包含表头）
                            df.to_csv(csv_file, mode='a', header=False, index=False, encoding='utf-8-sig')

                        processed_count += 1

                        # 每处理 10 支股票发送一次进度更新
                        if processed_count % 10 == 0:
                            logger.info(f"已處理 {processed_count}/{len(missing_stocks)} 支股票")
                            await update.message.reply_text(f"已處理 {processed_count}/{len(missing_stocks)} 支股票")

                        # 等待 0.5 秒再处理下一支股票
                        await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"處理股票 {stock_id} 時發生錯誤: {str(e)}")
                new_no_data_stocks.add(stock_id)
                continue

        # 更新没有数据的股票列表
        if new_no_data_stocks:
            no_data_stocks.update(new_no_data_stocks)
            with open(no_data_file, 'w', encoding='utf-8') as f:
                json.dump(list(no_data_stocks), f, ensure_ascii=False, indent=2)
            logger.info(f"新增 {len(new_no_data_stocks)} 支沒有數據的股票到記錄中")

        await update.message.reply_text(f"完成！共處理 {processed_count} 支股票數據")

    except Exception as e:
        logger.error(f"獲取股票 ROE 數據時發生錯誤: {str(e)}")
        await update.message.reply_text("處理過程中發生錯誤，請稍後再試")

# 添加新的常量
STOCK_PRICE_FILE = "stock_prices.json"

async def sync_stock_prices(update: Update, context: CallbackContext) -> None:
    """同步所有股票的最新價格並保存到 JSON 文件"""
    try:
        # 使用 get_taiwan_stock_list() 獲取股票列表
        stock_list = get_taiwan_stock_list()
        if not stock_list:
            await update.message.reply_text("無法獲取股票列表，請稍後再試")
            return

        logger.info(f"需要更新 {len(stock_list)} 支股票的價格")

        # 發送開始更新的訊息
        status_message = await update.message.reply_text("開始更新股票價格...")
        processed_count = 0
        updated_prices = {}

        # 處理每支股票
        for stock_id in stock_list:
            try:
                # 使用 get_current_stock_price 獲取價格
                current_price = await get_current_stock_price(stock_id)
                if current_price is not None:
                    updated_prices[stock_id] = current_price
                    processed_count += 1

                # 每處理 100 支股票更新一次進度
                if processed_count % 100 == 0:
                    progress = (processed_count / len(stock_list)) * 100
                    await status_message.edit_text(f"更新進度：{progress:.1f}% ({processed_count}/{len(stock_list)})")

                # 每支股票處理完後暫停一下
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"處理股票 {stock_id} 時發生錯誤: {str(e)}")
                continue

        # 保存更新後的價格數據
        with open(STOCK_PRICE_FILE, 'w', encoding='utf-8') as f:
            json.dump(updated_prices, f, ensure_ascii=False, indent=2)

        # 發送完成訊息
        await update.message.reply_text(f"股票價格更新完成！共更新 {len(updated_prices)} 支股票的價格")

    except Exception as e:
        logger.error(f"同步股票價格時發生錯誤: {str(e)}")
        await update.message.reply_text("處理過程中發生錯誤，請稍後再試")

def main():
    global app
    load_dotenv()
    
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    if not BOT_TOKEN:
        raise ValueError("未找到 BOT_TOKEN，請在 Heroku 環境變數設定 BOT_TOKEN")
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        app = Application.builder().token(BOT_TOKEN).build()

        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("get_roe_data", get_stock_roe_data))
        app.add_handler(CommandHandler("recommend_v2", recommend_v2))
        app.add_handler(CommandHandler("cancel_recommend", cancel_recommend))
        app.add_handler(CommandHandler("etf", etf))
        app.add_handler(CommandHandler("stock_estimate", stock_estimate))
        app.add_handler(CommandHandler("sync_stock_prices", sync_stock_prices))

        logger.info("Bot 已啟動並開始運行...")
        app.run_polling()
        
    except Exception as e:
        logger.error(f"運行時發生錯誤: {str(e)}")
        if app:
            app.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
