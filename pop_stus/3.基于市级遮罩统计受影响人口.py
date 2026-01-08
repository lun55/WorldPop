import os
import pandas as pd
import geopandas as gpd
from rasterstats import zonal_stats
from tqdm import tqdm
import re
import rasterio

def step3_per_mask_stats(overlay_root, population_root, counties_shp_path, output_root):
    # 1. 加载完整市级名录 (基准)
    print("正在加载完整市级名录...")
    counties_gdf = gpd.read_file(counties_shp_path)
    base_info = counties_gdf[['GID_2', 'NAME_1', 'NAME_2']].copy()
    
    pop_files = [f for f in os.listdir(population_root) if f.endswith('.tif')]
    noise_folders = [f for f in os.listdir(overlay_root) if os.path.isdir(os.path.join(overlay_root, f))]

    for folder in noise_folders:
        noise_year_match = re.search(r'20\d{2}', folder)
        if not noise_year_match: continue
        noise_year = noise_year_match.group()
        
        print(f"\n🚀 正在处理噪声组: {folder}")
        year_output_dir = os.path.join(output_root, noise_year)
        os.makedirs(year_output_dir, exist_ok=True)

        mask_dir = os.path.join(overlay_root, folder)
        mask_files = [f for f in os.listdir(mask_dir) if f.endswith(".shp")]
        
        # 预筛选该年份的人口文件
        current_pop_files = [f for f in pop_files if f"_{noise_year}_" in f]
        if not current_pop_files: continue

        # 获取该年份所有性别和年龄的组合，用于构建每个掩膜的底表
        pop_dims = []
        for pf in current_pop_files:
            pm = re.match(r'([a-zA-Z]+)_([fm])_(\d+)_(\d+)', pf)
            if pm: pop_dims.append((pm.group(2), pm.group(3)))
        unique_pop_dims = pd.DataFrame(pop_dims, columns=['Gender', 'Age_Group']).drop_duplicates()

        # 遍历每个掩膜文件 (例如: 40dB.shp, 45dB.shp)
        for mask_file in mask_files:
            threshold_match = re.search(r'(\d+)dB', mask_file)
            threshold = threshold_match.group(1) if threshold_match else "unknown"
            
            mask_path = os.path.join(mask_dir, mask_file)
            mask_gdf = gpd.read_file(mask_path)
            
            # --- 核心逻辑：为当前这个阈值构建全县 x 全性别年龄的底表 ---
            # 这里的 grid 只包含当前这一个 Threshold
            grid = base_info.assign(k=1).merge(unique_pop_dims.assign(k=1), on='k').drop('k', axis=1)
            grid['Threshold'] = threshold
            
            actual_records = []

            print(f"  -> 统计掩膜: {mask_file} (对应人口文件: {len(current_pop_files)}个)")
            
            for pop_tif_name in tqdm(current_pop_files, desc=f"     Pop Stats", leave=False):
                match = re.match(r'([a-zA-Z]+)_([fm])_(\d+)_(\d+)', pop_tif_name)
                if not match: continue
                _, gender, age, _ = match.groups()
                pop_tif_path = os.path.join(population_root, pop_tif_name)

                # 获取 nodata 并统计
                with rasterio.open(pop_tif_path) as src:
                    pop_nodata = src.nodata if src.nodata is not None else -99999

                if not mask_gdf.empty:
                    stats = zonal_stats(
                        mask_gdf, 
                        pop_tif_path, 
                        stats="sum", 
                        all_touched=False,
                        nodata=pop_nodata
                    )
                    
                    # 收集该人口文件下有值的县
                    for i in range(len(mask_gdf)):
                        pop_sum = stats[i]['sum']
                        if pop_sum and pop_sum > 0:
                            actual_records.append({
                                'GID_2': mask_gdf.iloc[i]['GID_2'],
                                'Gender': gender,
                                'Age_Group': age,
                                'Affected_Pop': pop_sum
                            })

            # --- 每一个掩膜文件合并一次并输出 ---
            if not grid.empty:
                if actual_records:
                    affected_df = pd.DataFrame(actual_records)
                    # 合并统计结果到全量底表
                    final_df = grid.merge(
                        affected_df, 
                        on=['GID_2', 'Gender', 'Age_Group'], 
                        how='left'
                    )
                else:
                    final_df = grid.copy()
                    final_df['Affected_Pop'] = 0

                final_df['Affected_Pop'] = final_df['Affected_Pop'].fillna(0)
                
                # 输出文件名：包含年份、组名和阈值
                clean_mask_name = mask_file.replace('.shp', '')
                output_csv = os.path.join(year_output_dir, f"Stats_{clean_mask_name}.csv")
                
                final_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

# 参数配置...
overlay_root = r"F:\机场噪音\County_Noise_Masks\美国"
population_root = r"F:\wordpop_USA\both\2023\fm"
counties_shp = r"USA\gadm41_USA_2.shp"
output_csv_root = r"F:\机场噪音\Final_Consolidated_Results\美国"

if __name__ == "__main__":
    step3_per_mask_stats(overlay_root, population_root, counties_shp, output_csv_root)