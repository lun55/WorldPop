import pandas as pd
import os

# -------------------------------
# 基本路径
# -------------------------------
base_folder = r"F:\机场噪音\Noise_Population_Stats_HighDB_Filtered"
pop_folder = r"F:\机场噪音\区域总人口"
output_folder = os.path.join(r"F:\机场噪音", "HighDB_Filtered_5_intervals_噪音汇总")
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# 级别、时间状态、年份
# -------------------------------
levels = ['L0', 'L1', 'L2']
time_types = ['oneday','night']
years = ['202110','202210','202310','总']  # '总'表示三年的噪音总影响

# -------------------------------
# 人口文件模板
# -------------------------------
pop_files = {
    'L0': os.path.join(pop_folder, "L0_total_pop_{}.csv"),
    'L1': os.path.join(pop_folder, "L1_total_pop_{}.csv"),
    'L2': os.path.join(pop_folder, "L2_total_pop_{}.csv")
}

# -------------------------------
# 找子文件夹下所有 CSV 文件
# -------------------------------
def list_csv_files(year, level, time_type):
    folder_path = os.path.join(base_folder, year, level)
    if not os.path.exists(folder_path):
        return []
    csv_files = []
    # 遍历子文件夹
    if year=="总":
        for sub in os.listdir(folder_path):
            sub_path = os.path.join(folder_path, sub)
            if os.path.isdir(sub_path) and sub.startswith(f"SEL_{time_type}"):
                files = [os.path.join(sub_path, f) for f in os.listdir(sub_path) if f.endswith(".csv")]
                csv_files.extend(files)
    else:
        for sub in os.listdir(folder_path):
            sub_path = os.path.join(folder_path, sub)
            if os.path.isdir(sub_path) and sub.startswith(f"SEL_{time_type}_{year}"):
                files = [os.path.join(sub_path, f) for f in os.listdir(sub_path) if f.endswith(".csv")]
                csv_files.extend(files)
    return csv_files

# -------------------------------
# 批量处理
# -------------------------------
for level in levels:
    # 合并列（gaul0/gaul1/gaul2）
    merge_cols = [col for col in pd.read_csv(pop_files[level].format('2021')).columns if col.startswith('gaul')]

    for time_type in time_types:
        for year in years:
            print(f"处理 {level} {time_type} {year} ...")

            # 获取文件列表
            files = list_csv_files(year, level, time_type)

            if not files:
                print(f"⚠️ 找不到文件: {level} {time_type} {year}")
                continue

            # 读取并合并 CSV
            df_list = [pd.read_csv(f) for f in files]
            df_all = pd.concat(df_list, ignore_index=True)

            # 按区域 + dB 汇总
            df_sum = df_all.groupby(merge_cols + ['dB_level'], as_index=False)['affected_pop'].sum()

            # 宽表转换，每个 dB 范围一列
            df_wide = df_sum.pivot_table(index=merge_cols, columns='dB_level', values='affected_pop', fill_value=0).reset_index()

            # dB 列排序
            dB_cols = sorted([col for col in df_wide.columns if col not in merge_cols])
            df_wide = df_wide[merge_cols + dB_cols]

            # 读取人口数据
            if year == '总':
                pop_year = '2023'  # 总用2023年人口
            else:
                pop_year = year[:4]  # 202110->2021
            pop_df = pd.read_csv(pop_files[level].format(pop_year))

            # 合并人口
            df_final = pd.merge(pop_df, df_wide, on=merge_cols, how='left')

            # 计算暴露比例
            for col in dB_cols:
                df_final[f"{col}_ratio"] = df_final[col] / df_final['total_pop']

            # 保存 CSV
            os.makedirs(os.path.join(output_folder, year,level), exist_ok=True)
            out_file = os.path.join(output_folder, year,level, f"{level}_{time_type}_{year}_exposure.csv")
            df_final.to_csv(out_file, index=False)
            print(f"完成: {out_file}")
