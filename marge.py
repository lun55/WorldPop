import os
import pandas as pd
import glob

# ======== 配置 ========
results_folder = r"E:\WordPop\results"
output_path = os.path.join(results_folder, "total_population_by_city.csv")

# ======== 读取所有年龄段 CSV ========
csv_files = glob.glob(os.path.join(results_folder, "population_age_*.csv"))
print(f"Found {len(csv_files)} age-group CSV files.")

# ======== 合并所有年龄段 ========
dfs = []
for file in csv_files:
    age_group = os.path.basename(file).split("_")[2].replace(".csv", "")
    df = pd.read_csv(file)
    df = df[["GID_2", "population_sum"]].rename(columns={"population_sum": f"age_{age_group}"})
    dfs.append(df)

# ======== 合并所有年龄段列 ========
merged = dfs[0]
for df in dfs[1:]:
    merged = merged.merge(df, on="GID_2", how="outer")

# ======== 计算总人口 ========
age_columns = [col for col in merged.columns if col.startswith("age_")]
merged["total_population"] = merged[age_columns].sum(axis=1, skipna=True)

# ======== 添加城市名称信息 ========
sample_df = pd.read_csv(csv_files[0])
name_info = sample_df[["GID_2", "NAME_2", "NAME_1"]].drop_duplicates()
final = merged.merge(name_info, on="GID_2")

# ======== 保存结果 ========
final[["GID_2", "NAME_2", "NAME_1", "total_population"]].to_csv(output_path, index=False)
print(f"✅ Total population by city saved to: {output_path}")