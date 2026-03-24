import pandas as pd
import os

# 1. 加载数据
file_path = r"F:\机场噪音\PM2.5\PM2.5_影响汇总\202210\L1\L1_202210_exposure.csv"
df = pd.read_csv(file_path)

# 2. 定义原始的区间列名顺序
interval_cols = [
    '0_5', '5_10', '10_15', '15_25', '25_35', 
    '35_50', '50_75', '75_115', '115_150', '150_185'
]

# 3. 定义阈值下限
thresholds = [0, 5, 10, 15, 25, 35, 50, 75, 115, 150]

# 4. 循环计算累积暴露数
for i, t in enumerate(thresholds):
    cols_to_sum = interval_cols[i:]
    
    col_name_pop = f"ge_{t}_pop"
    col_name_ratio = f"ge_{t}_ratio"
    
    # 计算累积值
    df[col_name_pop] = df[cols_to_sum].sum(axis=1)
    df[col_name_ratio] = df[col_name_pop] / df['total_pop']

# 5. 筛选最终需要的列
# 保留：所有的 gaul 属性列、总人口列，以及新生成的 ge_ 累积列
keep_cols = [c for c in df.columns if c.startswith('gaul') or c == 'total_pop' or c.startswith('ge_')]

# 重新赋值给 df，彻底丢弃旧的分段字段
df_final = df[keep_cols].copy()

# 6. 保存结果
output_path = r"F:\机场噪音\PM2.5\PM2.5暴露数量\L1_202210_exposure_with_thresholds.csv"
df_final.to_csv(output_path, index=False)

print(f"处理完成！已删除原始分段字段，结果已保存至: {output_path}")
print(df_final.head())