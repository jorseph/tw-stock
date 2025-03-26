import pandas as pd
import numpy as np

# 讀取 CSV 文件
df = pd.read_csv('stock_roe_data.csv')

# 顯示基本信息
print("CSV 文件基本信息：")
print(df.info())
print("\n前 5 行數據：")
print(df.head())

# 檢查數值列的統計信息
print("\n數值列的統計信息：")
print(df.describe())

# 檢查是否有空值
print("\n空值統計：")
print(df.isnull().sum())

# 檢查 PER 和 PBR 的範圍
print("\nPER 範圍：")
print(df['PER'].value_counts().head())
print("\nPBR 範圍：")
print(df['PBR'].value_counts().head()) 