import sqlite3
import json

DB_PATH = "stock_data.db"

def init_db():
    """初始化 SQLite 資料表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 建立財報數據表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_reports (
            stock_no TEXT,
            date TEXT,
            data TEXT,
            PRIMARY KEY (stock_no, date)
        )
    """)

    # 建立股票列表表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_list (
            stock_no TEXT PRIMARY KEY,
            name TEXT,
            industry TEXT,
            market TEXT
        )
    """)

    conn.commit()
    conn.close()

def save_stock_list(stock_list):
    """將股市列表存入資料庫 (只存一次)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT OR IGNORE INTO stock_list (stock_no, name, industry, market) 
        VALUES (?, ?, ?, ?)
    """, stock_list)

    conn.commit()
    conn.close()

def load_stock_list():
    """從資料庫加載股市列表"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT stock_no, name, industry, market FROM stock_list")
    rows = cursor.fetchall()
    conn.close()

    return rows if rows else None
