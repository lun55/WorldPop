import os
import re
import pandas as pd
from tqdm import tqdm

# ---------------- 用户配置 ----------------
LEVEL = 2
YEAR = "202310"
TOTAL_POP_ROOT = r"F:\机场噪音\区域总人口"
NOISE_POP_ROOT = rf"F:\机场噪音\Noise_Population_Stats_矫正\{YEAR}\L{LEVEL}"
FINAL_OUT_ROOT = rf"F:\机场噪音\Final_Analysis_矫正\{YEAR}"
os.makedirs(FINAL_OUT_ROOT, exist_ok=True)

# 挂接键
if LEVEL == 0:
    JOIN_KEYS = ["gaul0_name"]
elif LEVEL == 1:
    JOIN_KEYS = ["gaul0_name", "gaul1_name"]
else:
    JOIN_KEYS = ["gaul0_name", "gaul1_name", "gaul2_name"]

# ---------------- 1. 加载所有噪音数据 ----------------
print("正在读取各文件夹下的统计数据...")
raw_list = []
noise_folders = [d for d in os.listdir(NOISE_POP_ROOT) if os.path.isdir(os.path.join(NOISE_POP_ROOT, d))]

for folder_name in tqdm(noise_folders, desc="扫描目录"):
    folder_path = os.path.join(NOISE_POP_ROOT, folder_name)
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    
    for csv_name in csv_files:
        df_noise = pd.read_csv(os.path.join(folder_path, csv_name))
        if df_noise.empty: continue
        
        # 提取元数据用于构造列名
        db_level = str(df_noise["dB_level"].iloc[0])
        time_type = str(df_noise["time_type"].iloc[0])
        # 组合列名：噪音源_昼夜_分贝
        col_id = f"{folder_name}_{time_type}_{db_level}dB"
        
        df_noise["column_tag"] = col_id
        # 确保 year_used 是字符串格式
        df_noise["year_used"] = df_noise["year_used"].astype(str)
        raw_list.append(df_noise)

if not raw_list:
    print("❌ 未找到任何统计数据，请检查路径。")
    exit()

all_noise_data = pd.concat(raw_list, ignore_index=True)

# ---------------- 2. 按年份循环生成大表 ----------------
years_available = all_noise_data["year_used"].unique()

for year in years_available:
    print(f"\n📅 正在处理 {year} 年度汇总表...")
    
    # A. 提取当年数据并透视 (行转列)
    year_noise_data = all_noise_data[all_noise_data["year_used"] == year]
    
    pivot_df = year_noise_data.pivot_table(
        index=JOIN_KEYS, 
        columns="column_tag", 
        values="affected_pop",
        aggfunc='first'
    ).reset_index().fillna(0)

    # B. 读取当年的总人口基准表
    base_pop_file = os.path.join(TOTAL_POP_ROOT, f"L{LEVEL}_total_pop_{year}.csv")
    if not os.path.exists(base_pop_file):
        print(f"⚠️ 跳过: 找不到 {year} 年的总人口基准表 {base_pop_file}")
        continue
    
    base_pop_df = pd.read_csv(base_pop_file)
    for col in JOIN_KEYS:
        base_pop_df[col] = base_pop_df[col].astype(str)
        pivot_df[col] = pivot_df[col].astype(str)

    # C. 挂接总人口 (Left Join)
    # 将透视后的噪音数据挂载到总人口表上，这样可以保留该年所有行政区
    final_year_df = pd.merge(base_pop_df, pivot_df, on=JOIN_KEYS, how='left').fillna(0)

    # D. 动态计算所有场景的影响比例
    noise_cols = [c for c in pivot_df.columns if c.endswith("dB")]
    
    for nc in noise_cols:
        ratio_col_name = nc.replace("dB", "dB_ratio")
        # 防止分母为 0
        final_year_df[ratio_col_name] = 0.0
        mask = final_year_df['total_pop'] > 0
        final_year_df.loc[mask, ratio_col_name] = (
            final_year_df.loc[mask, nc] / final_year_df.loc[mask, 'total_pop']
        ).clip(upper=1.0) # 修正因算法产生的极微小溢出

    # E. 导出年度大表
    out_name = f"L{LEVEL}_Global_Noise_Impact_{year}_Wide.csv"
    final_year_df.to_csv(os.path.join(FINAL_OUT_ROOT, out_name), index=False)
    print(f"✅ 已生成: {out_name} (共 {len(final_year_df)} 行)")

print(f"\n🎉 任务全部完成！结果存储在: {FINAL_OUT_ROOT}")