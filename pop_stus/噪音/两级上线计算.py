import pandas as pd
import numpy as np
import os
def aggregate_noise_to_l1_revised(df):
    group_cols = ['gaul0_name', 'gaul1_name']
    pop_impact_cols = [c for c in df.columns if 'dB' in c and '_ratio' not in c]
    
    # --- 修改点：在基础聚合中增加 count() 来统计 L2 城市总数 ---
    l1_df = df.groupby(group_cols).agg({
        'total_pop': 'sum',
        'gaul2_name': 'count'  # 统计该 L1 下有多少个 L2 记录
    }).reset_index()
    l1_df = l1_df.rename(columns={'gaul2_name': 'total_city_count'})

    for col in pop_impact_cols:
        impact_sum = df.groupby(group_cols)[col].sum().rename(f"{col}_sum")
        
        mask = df[col] > 0
        df_affected = df[mask]
        
        city_count = df_affected.groupby(group_cols).size().rename(f"{col}_city_count")
        affected_city_pop = df_affected.groupby(group_cols)['total_pop'].sum().rename(f"{col}_affected_city_pop")
        
        temp_stats = pd.concat([impact_sum, city_count, affected_city_pop], axis=1).reset_index()
        l1_df = l1_df.merge(temp_stats, on=group_cols, how='left')
        
        l1_df[f"{col}_sum"] = l1_df[f"{col}_sum"].fillna(0)
        l1_df[f"{col}_city_count"] = l1_df[f"{col}_city_count"].fillna(0).astype(int)
        l1_df[f"{col}_affected_city_pop"] = l1_df[f"{col}_affected_city_pop"].fillna(0)

        ratio_name = f"{col}_ratio"
        l1_df[ratio_name] = l1_df[f"{col}_sum"] / l1_df[f"{col}_affected_city_pop"]
        l1_df[ratio_name] = l1_df[ratio_name].replace([np.inf, -np.inf], 0).fillna(0)

    rename_dict = {f"{col}_sum": col for col in pop_impact_cols}
    l1_df = l1_df.rename(columns=rename_dict)
    return l1_df

def aggregate_noise_to_l0(df):
    group_cols = ['gaul0_name']
    pop_impact_cols = [c for c in df.columns if 'dB' in c and '_ratio' not in c]
    
    # --- 修改点：在基础聚合中增加 count() 统计全国 L2 城市总数 ---
    l0_df = df.groupby(group_cols).agg({
        'total_pop': 'sum',
        'gaul2_name': 'count'
    }).reset_index()
    l0_df = l0_df.rename(columns={'gaul2_name': 'total_city_count'})

    for col in pop_impact_cols:
        impact_sum = df.groupby(group_cols)[col].sum().rename(f"{col}_sum")
        
        mask = df[col] > 0
        df_affected = df[mask]
        
        city_count = df_affected.groupby(group_cols).size().rename(f"{col}_city_count")
        affected_city_pop = df_affected.groupby(group_cols)['total_pop'].sum().rename(f"{col}_affected_city_pop")
        
        temp_stats = pd.concat([impact_sum, city_count, affected_city_pop], axis=1).reset_index()
        l0_df = l0_df.merge(temp_stats, on=group_cols, how='left')
        
        l0_df[f"{col}_sum"] = l0_df[f"{col}_sum"].fillna(0)
        l0_df[f"{col}_city_count"] = l0_df[f"{col}_city_count"].fillna(0).astype(int)
        l0_df[f"{col}_affected_city_pop"] = l0_df[f"{col}_affected_city_pop"].fillna(0)

        ratio_name = f"{col}_ratio"
        l0_df[ratio_name] = l0_df[f"{col}_sum"] / l0_df[f"{col}_affected_city_pop"]
        l0_df[ratio_name] = l0_df[ratio_name].replace([np.inf, -np.inf], 0).fillna(0)

    rename_dict = {f"{col}_sum": col for col in pop_impact_cols}
    l0_df = l0_df.rename(columns=rename_dict)
    return l0_df


df = pd.read_csv(r"F:\机场噪音\Final_Analysis_New_矫正\总\L2_Global_Noise_Impact_2023_Wide.csv")
result_l1 = aggregate_noise_to_l1_revised(df)
result_l1.to_csv(r"F:\机场噪音\Final_Analysis_New_L0L1上限\总\L1_Aggregation_Global_Noise_Impact_2023.csv", index=False) 

df = pd.read_csv(r"F:\机场噪音\Final_Analysis_New_矫正\总\L2_Global_Noise_Impact_2023_Wide.csv")
result_l1 = aggregate_noise_to_l0(df)
result_l1.to_csv(r"F:\机场噪音\Final_Analysis_New_L0L1上限\总\L0_Aggregation_Global_Noise_Impact_2023.csv", index=False) 

if __name__ == "__main__":
    years = [2021, 2022, 2023]
    base_input_path = r"F:\机场噪音\Final_Analysis_New_矫正"
    base_output_path = r"F:\机场噪音\Final_Analysis_New_L0L1上限"

    for year in years:
        folder_name = f"{year}10"
        input_dir = os.path.join(base_input_path, folder_name)
        output_dir = os.path.join(base_output_path, folder_name)
        os.makedirs(output_dir, exist_ok=True)
        
        input_file = os.path.join(input_dir, f"L2_Global_Noise_Impact_{year}_Wide.csv")
        
        if os.path.exists(input_file):
            print(f"\n🚀 正在处理 {year} 年全球数据聚合...")
            df = pd.read_csv(input_file)
            
            # --- 聚合到 L1 (省级) ---
            result_l1 = aggregate_noise_to_l1_revised(df)
            output_file_l1 = os.path.join(output_dir, f"L1_Aggregation_Global_Noise_Impact_{year}.csv")
            result_l1.to_csv(output_file_l1, index=False)
            print(f"  ✅ L1 聚合完成")

            # --- 聚合到 L0 (国家级) ---
            result_l0 = aggregate_noise_to_l0(df)
            output_file_l0 = os.path.join(output_dir, f"L0_Aggregation_Global_Noise_Impact_{year}.csv")
            result_l0.to_csv(output_file_l0, index=False)
            print(f"  ✅ L0 聚合完成")
            
        else:
            print(f"❌ 未找到文件: {input_file}")

    print("\n🎉 2021-2023 年度全级别聚合处理全部完成！")