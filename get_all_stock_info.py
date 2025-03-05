import pandas as pd

# 讀取 CSV 檔案
file_path = "./StockList.csv"  # 請替換成你的 CSV 檔案路徑
df = pd.read_csv(file_path, encoding="utf-8")

# 顯示前幾行數據
print(df.head())

# 檢查 DataFrame 資訊
print(df.info())