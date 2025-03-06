import pandas as pd
import logging
import os
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackContext

# 設定日誌
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# 載入 CSV 檔案
CSV_FILE = "Calculated_Stock_Values.csv"  # 確保檔案路徑正確
df = pd.read_csv(CSV_FILE)

# 處理代號為字符串，避免數據類型問題
df["代號"] = df["代號"].astype(str)

# 查詢股票資訊
async def stock(update: Update, context: CallbackContext) -> None:
    if not context.args:
        await update.message.reply_text("請輸入股票代號，例如：/stock 3008")
        return

    stock_id = context.args[0]
    stock_info = df[df["代號"] == stock_id]

    if stock_info.empty:
        await update.message.reply_text(f"找不到股票代號 {stock_id} 的資訊")
        return

    info = stock_info.iloc[0]  # 取得第一筆資料
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
        (df["平均毛利(%)"] > 30)  # 毛利率 > 30%
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

def main():
    # 讀取 Heroku 環境變數
    BOT_TOKEN = os.getenv("BOT_TOKEN")

    if not BOT_TOKEN:
        raise ValueError("未找到 BOT_TOKEN，請在 Heroku 環境變數設定 BOT_TOKEN")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stock", stock))
    app.add_handler(CommandHandler("recommend", recommend))

    app.run_polling()

if __name__ == "__main__":
    main()
