import pandas as pd

# 讀取合併後的 CSV 檔案
file_path = "./Merged_Inner.csv"
df = pd.read_csv(file_path)

# 移除 "排名" 欄位（如果存在）
if "排名" in df.columns:
    df = df.drop(columns=["排名"])

# 正確合併名稱（優先使用名稱_x，若名稱_x 無效則使用 名稱_y）
df["名稱"] = df["名稱_x"].fillna(df["名稱_y"]).str.replace(r"\*", "", regex=True)
df = df.drop(columns=["名稱_x", "名稱_y"])

# 合併成交欄位，取成交_x為主，若無則取成交_y
df["成交"] = df["成交_x"].fillna(df["成交_y"])
df = df.drop(columns=["成交_x", "成交_y"])

# 確保 "代號" 只包含數字
df["代號"] = df["代號"].astype(str).str.extract(r'(\d+)')

# 確保數據為數值型態
df["平均ROE(%)"] = pd.to_numeric(df["平均ROE(%)"], errors='coerce')
df["成交"] = pd.to_numeric(df["成交"], errors='coerce')
df["目前PBR"] = pd.to_numeric(df["目前PBR"], errors='coerce')
df["平均最低PER"] = pd.to_numeric(df["平均最低PER"], errors='coerce')
df["平均最高PER"] = pd.to_numeric(df["平均最高PER"], errors='coerce')
df["平均PER"] = pd.to_numeric(df["平均PER"], errors='coerce')
df["平均財報評分"] = pd.to_numeric(df["平均財報評分"], errors="coerce")

# 計算 EPS（根據 PBR 和 股價）
df["計算EPS"] = df["平均ROE(%)"] * (df["成交"] / df["目前PBR"]) / 100

# 計算合理股價範圍
df["最低合理股價"] = (df["計算EPS"] * df["平均最低PER"]).round(2)
df["平均合理股價"] = (df["計算EPS"] * df["平均PER"]).round(2)
df["最高合理股價"] = (df["計算EPS"] * df["平均最高PER"]).round(2)

# 儲存結果
output_path = "./Calculated_Stock_Values.csv"
df.to_csv(output_path, index=False)

# 顯示前 10 筆數據
print(df.head(10))

print(f"計算完成，結果已儲存至 {output_path}")
