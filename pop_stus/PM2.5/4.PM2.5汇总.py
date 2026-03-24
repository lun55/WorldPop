import pandas as pd
import os
import re
# -------------------------------
# 基本路径
# -------------------------------
base_folder = r"F:\机场噪音\PM2.5\PM2.5_人口统计\GHAP_PM2.5_M1K_202210"
pop_folder = r"F:\机场噪音\区域总人口"
output_folder = os.path.join(r"F:\机场噪音\PM2.5", "PM2.5_影响汇总")
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# 级别、时间状态、年份
# -------------------------------
levels = ['L0','L1',"L2"]
years = ['202210']  # '总'表示三年的噪音总影响

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
def list_csv_files(level):
    folder_path = os.path.join(base_folder, level)
    csv_files = []
    if not os.path.exists(folder_path): return []
    for root, dirs, files in os.walk(folder_path):
        for f in files:
            if f.endswith(".csv"):
                csv_files.append(os.path.join(root, f))
    return csv_files

# --- 新增：PM区间排序辅助函数 ---
def sort_pm_ranges(range_list):
    # 将 "0_5" 转换成数字列表 [0, 5] 进行排序
    def extract_numbers(s):
        nums = re.findall(r'\d+', s)
        return [int(n) for n in nums] if nums else [999, 999]
    return sorted(range_list, key=extract_numbers)

# -------------------------------
# 批量处理
# -------------------------------
for level in levels:
    # 动态获取当前级别的区域列名 (gaul0_name等)
    # 注意：这里读取一个样本文件来确认列名
    sample_pop_file = pop_files[level].format('2022') # 改为你的实际年份
    merge_cols = [col for col in pd.read_csv(sample_pop_file).columns if col.startswith('gaul')]

    for year in years:
        print(f"处理 {level} {year} ...")

        files = list_csv_files(level)
        if not files:
            print(f"⚠️ 找不到文件: {level} {year}")
            continue

        # 1. 读取并合并所有小 CSV
        df_list = []
        for f in files:
            temp_df = pd.read_csv(f)
            # 【关键修改】如果之前的脚本存的是 PM_range，这里确保字段统一
            if 'PM_range' not in temp_df.columns and 'PM_level' in temp_df.columns:
                temp_df.rename(columns={'PM_level': 'PM_range'}, inplace=True)
            df_list.append(temp_df)
            
        df_all = pd.concat(df_list, ignore_index=True)

        # 2. 按区域 + PM_range 汇总
        # 将这里的 PM_level 修改为 PM_range
        df_sum = df_all.groupby(merge_cols + ['PM_range'], as_index=False)['affected_pop'].sum()

        # 3. 宽表转换
        df_wide = df_sum.pivot_table(
            index=merge_cols, 
            columns='PM_range', 
            values='affected_pop', 
            fill_value=0
        ).reset_index()

        # 4. 【关键修改】PM 列进行逻辑排序 (0_5, 5_10, 10_15...)
        pm_cols = [col for col in df_wide.columns if col not in merge_cols]
        pm_cols = sort_pm_ranges(pm_cols) 
        df_wide = df_wide[merge_cols + pm_cols]

        # 5. 合并总人口数据
        pop_year = year[:4] 
        pop_data_path = pop_files[level].format(pop_year)
        if not os.path.exists(pop_data_path):
            print(f"❌ 缺少人口底图数据: {pop_data_path}")
            continue
            
        pop_df = pd.read_csv(pop_data_path)
        df_final = pd.merge(pop_df, df_wide, on=merge_cols, how='left').fillna(0)

        # 6. 计算暴露比例
        for col in pm_cols:
            # 避免除以 0 的情况
            df_final[f"{col}_ratio"] = df_final[col] / df_final['total_pop'].replace(0, 1)

        # 7. 保存结果
        save_path = os.path.join(output_folder, year, level)
        os.makedirs(save_path, exist_ok=True)
        out_file = os.path.join(save_path, f"{level}_{year}_exposure.csv")
        df_final.to_csv(out_file, index=False)
        print(f"✅ 完成汇总并已排序: {out_file}")
