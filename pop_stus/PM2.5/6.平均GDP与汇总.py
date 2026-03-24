import pandas as pd
import os
import re
import numpy as np

# -------------------------------
# 路径配置
# -------------------------------
pop_base_folder = r"F:\机场噪音\PM2.5\PM2.5_人口统计"
gdp_base_folder = r"F:\机场噪音\PM2.5\PM2.5_GDP_Stats" 
output_folder = os.path.join(r"F:\机场噪音\PM2.5", "PM2.5_GDP汇总")
os.makedirs(output_folder, exist_ok=True)

levels = ['L0','L1',"L2"]
years = ['202210']

# -------------------------------
# 辅助函数
# -------------------------------
def list_csv_files(base_folder, level):
    # 修正：直接指向具体的月份子文件夹，防止递归读取到其他月份
    folder_path = os.path.join(base_folder, f"GHAP_PM2.5_M1K_{years[0]}", level)
    csv_files = []
    if not os.path.exists(folder_path): 
        print(f"路径不存在: {folder_path}")
        return []
    for root, dirs, files in os.walk(folder_path):
        for f in files:
            if f.endswith(".csv"):
                csv_files.append(os.path.join(root, f))
    return csv_files

def sort_pm_ranges(range_list):
    def extract_numbers(s):
        # 提取 "ave_gdp_0_5" 中的数字部分
        nums = re.findall(r'\d+', s)
        return [int(n) for n in nums] if nums else [999, 999]
    return sorted(range_list, key=extract_numbers)

# -------------------------------
# 批量处理
# -------------------------------
for level in levels:
    for year in years:
        print(f">>> 正在计算 GDP 聚合: {level} {year} ...")

        pop_files = list_csv_files(pop_base_folder, level)
        gdp_files = list_csv_files(gdp_base_folder, level)

        if not pop_files or not gdp_files:
            continue

        # 1. 处理人口数据
        df_pop_all = pd.concat([pd.read_csv(f) for f in pop_files], ignore_index=True)
        # 统一列名大小写
        df_pop_all.columns = [c.replace('pm_range', 'PM_range') for c in df_pop_all.columns]
        
        merge_cols = [col for col in df_pop_all.columns if col.startswith('gaul')]
        df_pop_sum = df_pop_all.groupby(merge_cols + ['PM_range'], as_index=False)['affected_pop'].sum()

        # 2. 处理 GDP 数据
        df_gdp_all = pd.concat([pd.read_csv(f) for f in gdp_files], ignore_index=True)
        df_gdp_all.columns = [c.replace('pm_range', 'PM_range') for c in df_gdp_all.columns]
        
        # 自动识别 GDP 数值列（可能是 affected_gdp 或 sum）
        gdp_val_col = 'affected_gdp' if 'affected_gdp' in df_gdp_all.columns else 'sum'
        df_gdp_sum = df_gdp_all.groupby(merge_cols + ['PM_range'], as_index=False)[gdp_val_col].sum()

        # 3. 合并并计算人均 GDP
        df_merged = pd.merge(df_pop_sum, df_gdp_sum, on=merge_cols + ['PM_range'], how='inner')
        
        # 使用 np.where 避免除以零错误
        df_merged['ave_gdp'] = np.where(
            df_merged['affected_pop'] > 0, 
            df_merged[gdp_val_col] / df_merged['affected_pop'], 
            0
        )
        
        # 4. 宽表转换
        df_wide = df_merged.pivot_table(
            index=merge_cols, 
            columns='PM_range', 
            values='ave_gdp', 
            fill_value=0
        ).reset_index()

        # 5. 排序与重命名
        raw_pm_cols = [col for col in df_wide.columns if col not in merge_cols]
        # 应用逻辑排序 (0_5, 5_10...)
        sorted_pm_cols = sort_pm_ranges(raw_pm_cols)
        
        # 构建重命名映射
        rename_dict = {col: f"ave_gdp_{col}" for col in sorted_pm_cols}
        
        # 重新排序列并应用重命名
        df_wide = df_wide[merge_cols + sorted_pm_cols]
        df_wide.rename(columns=rename_dict, inplace=True)

        # 6. 保存
        final_out_dir = os.path.join(output_folder, year, level)
        os.makedirs(final_out_dir, exist_ok=True)
        out_file = os.path.join(final_out_dir, f"{level}_{year}_ave_gdp_stats.csv")
        
        df_wide.to_csv(out_file, index=False, encoding="utf-8")
        print(f"✅ 完成写入: {out_file}")

print("\n🎉 所有 PM2.5 关联 GDP 任务已完成！")