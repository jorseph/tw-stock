import pandas as pd

# 讀取 CSV 檔案
file1_path = "./StockListPer.csv"
file2_path = "./StockList.csv"

df1 = pd.read_csv(file1_path)
df2 = pd.read_csv(file2_path)

# 確保 "代號" 欄位類型一致，轉換為字串
df1["代號"] = df1["代號"].astype(str)
df2["代號"] = df2["代號"].astype(str)

# 檢查各自的筆數
print("File1 筆數:", len(df1))
print("File2 筆數:", len(df2))

# 內聯合併 (只保留兩者皆有的 "代號")
df_inner = pd.merge(df1, df2, on="代號", how="inner")
print("內聯合併後筆數:", len(df_inner))

# 外聯合併 (保留所有 "代號"，無匹配的填充 NaN)
df_outer = pd.merge(df1, df2, on="代號", how="outer")
print("外聯合併後筆數:", len(df_outer))

# 儲存合併結果
df_inner.to_csv("./Merged_Inner.csv", index=False)
df_outer.to_csv("./Merged_Outer.csv", index=False)

print("合併完成，內聯合併和外聯合併結果已儲存。")
