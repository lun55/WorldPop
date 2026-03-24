import pandas as pd
import os

# -------------------------------
# 路径配置 (请根据实际情况修改)
# -------------------------------
# 1. 之前生成的人口汇总结果路径 (从中获取 affected_pop)
pop_base_folder = r"F:\机场噪音\Noise_Population_Stats_HighDB_Filtered"
# 2. GDP 原始数据路径 (目录结构与人口一致)
gdp_base_folder = r"F:\机场噪音\Noise_GDP_Stats_HighDB_Filtered" 
output_folder = os.path.join(r"F:\机场噪音", "HighDB_Filtered_GDP汇总")

os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# 配置参数
# -------------------------------
levels = ['L0', 'L1', 'L2']
time_types = ['oneday', 'night']
years = ['202110', '202210', '202310', '总']

def list_csv_files(root_dir, year, level, time_type):
    folder_path = os.path.join(root_dir, year, level)
    if not os.path.exists(folder_path):
        return []
    csv_files = []
    # 兼容“总”文件夹命名逻辑
    prefix = f"SEL_{time_type}" if year == "总" else f"SEL_{time_type}_{year}"
    
    for sub in os.listdir(folder_path):
        sub_path = os.path.join(folder_path, sub)
        if os.path.isdir(sub_path) and sub.startswith(prefix):
            files = [os.path.join(sub_path, f) for f in os.listdir(sub_path) if f.endswith(".csv")]
            csv_files.extend(files)
    return csv_files

# -------------------------------
# 批量处理
# -------------------------------
for level in levels:
    for time_type in time_types:
        for year in years:
            print(f">>> 正在计算 GDP 聚合: {level} {time_type} {year} ...")

            # 1. 获取对应的文件列表
            pop_files = list_csv_files(pop_base_folder, year, level, time_type)
            gdp_files = list_csv_files(gdp_base_folder, year, level, time_type)

            if not pop_files or not gdp_files:
                print(f"⚠️ 跳过：人口或GDP文件缺失 ({level} {time_type} {year})")
                continue

            # 2. 读取并合并人口数据
            df_pop_all = pd.concat([pd.read_csv(f) for f in pop_files], ignore_index=True)
            # 确定地理维度列
            merge_cols = [col for col in df_pop_all.columns if col.startswith('gaul')]
            
            # 汇总人口：按区域和分贝
            df_pop_sum = df_pop_all.groupby(merge_cols + ['dB_level'], as_index=False)['affected_pop'].sum()

            # 3. 读取并合并 GDP 数据
            # 假设 GDP 文件中的数值列名为 'affected_gdp' 或类似的，这里设为变量方便修改
            df_gdp_all = pd.concat([pd.read_csv(f) for f in gdp_files], ignore_index=True)
            # 注意：此处假设 GDP 文件中的 GDP 总值列名为 'affected_gdp'
            gdp_val_col = 'affected_gdb' 
            df_gdp_sum = df_gdp_all.groupby(merge_cols + ['dB_level'], as_index=False)[gdp_val_col].sum()

            # 4. 合并人口与 GDP
            df_merged = pd.merge(df_pop_sum, df_gdp_sum, on=merge_cols + ['dB_level'], how='inner')

            # 5. 计算人均 GDP (Average GDP per noise interval)
            # 这里的计算逻辑是：该分贝区间内的总GDP / 该区间内的总人口
            # 只有当人口 > 0 时才计算，否则填充 0
            import numpy as np
            df_merged['ave_gdp'] = np.where(
                df_merged['affected_pop'] > 0, 
                df_merged[gdp_val_col] / df_merged['affected_pop'], 
                0
            )
            
            # 6. 宽表转换 (针对 ave_gdp)
            # 索引是地理区域，列是各个分贝等级
            df_wide = df_merged.pivot_table(
                index=merge_cols, 
                columns='dB_level', 
                values='ave_gdp', 
                fill_value=0
            ).reset_index()

            # 7. 重命名列名以区分 (例如 45dB -> ave_gdp_45dB)
            dB_cols = [col for col in df_wide.columns if col not in merge_cols]
            rename_dict = {col: f"ave_gdp_{col}" for col in dB_cols}
            df_wide.rename(columns=rename_dict, inplace=True)

            # 8. 保存结果
            final_out_dir = os.path.join(output_folder, year, level)
            os.makedirs(final_out_dir, exist_ok=True)
            out_file = os.path.join(final_out_dir, f"{level}_{time_type}_{year}_ave_gdp_stats.csv")
            
            df_wide.to_csv(out_file, index=False, encoding="utf-8")
            print(f"✅ 完成写入: {out_file}")

print("\n🎉 所有 GDP 聚合任务已完成！")