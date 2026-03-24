import rasterio
from rasterio.features import shapes
import geopandas as gpd
import pandas as pd
import numpy as np
import os

def process_all_noise_tifs(input_folder, output_root, thresholds):
    """
    自动扫描目录下所有TIF，针对每个阈值生成对应的 SHP 矢量。
    """
    # 扫描目录下所有 tif 文件
    tif_files = [f for f in os.listdir(input_folder) if f.lower().endswith(('.tif', '.tiff'))]
    
    if not tif_files:
        print("❌ 未找到 TIF 文件，请检查路径。")
        return

    for tif_name in tif_files:
        print(f"\n开始处理原始影像: {tif_name}")
        tif_path = os.path.join(input_folder, tif_name)
        
        # 建立基于文件名的子文件夹，例如 SEL_night_202110
        file_base_name = os.path.splitext(tif_name)[0]
        tif_output_dir = os.path.join(output_root, file_base_name)
        os.makedirs(tif_output_dir, exist_ok=True)

        with rasterio.open(tif_path) as src:
            noise_data = src.read(1)
            # 预处理：将 nodata 转为极小值
            nodata_val = src.nodata if src.nodata is not None else -9999
            noise_data[noise_data == nodata_val] = 0

            for threshold in thresholds:
                output_shp_path = os.path.join(tif_output_dir, f"{file_base_name}_{threshold}dB.shp")
                
                # 如果文件已存在则跳过，方便断点续传
                if os.path.exists(output_shp_path):
                    print(f"  ⏭  {threshold}dB 已存在，跳过。")
                    continue

                # 掩模：选择大于等于阈值的像素
                mask = (noise_data >= threshold)
                
                if not np.any(mask):
                    print(f"  ⚠  {threshold}dB 下无数据。")
                    continue

                # 1. 栅格转矢量 (基于 3857)
                results = (
                    {'properties': {'dB_level': threshold}, 'geometry': s}
                    for s, v in shapes(noise_data, mask=mask, transform=src.transform)
                )
                
                # 2. 创建 GeoDataFrame
                gdf_3857 = gpd.GeoDataFrame.from_features(list(results), crs=src.crs)
                
                # 3. 融合并重投影至 4326
                # 注意：dissolve 可以极大简化多边形数量，避免后续计算过慢
                gdf_4326 = gdf_3857.dissolve(by='dB_level').explode(index_parts=False).reset_index(drop=True)
                gdf_4326 = gdf_4326.to_crs("EPSG:4326")
                # 4. 保存为 Shapefile
                # Shapefile 不支持长字段名，dB_level 会被缩写，但没关系
                gdf_4326.to_file(output_shp_path, driver='ESRI Shapefile', encoding='utf-8')
                print(f"已生成: {threshold}dB 矢量文件")

# === 配置参数 ===
base_input = r"F:\机场噪音\全球噪音_ALL_测试"          # 四个文件夹的父目录
output_root = r"F:\机场噪音\County_Noise_Masks_New\ALL_40_45"  # 总输出根目录

# 四个时段：文件夹名 + 对应阈值
periods = [
    ("oneday",    [45, 50, 55]),   # 白天
    ("night",     [40, 45, 50]),   # 夜间
    (r"all\oneday", [45, 50, 55]), # 汇总-白天
    (r"all\night",  [40, 45, 50])  # 汇总-夜间
]

# === 批量循环 ===
for folder_name, thresholds in periods:
    input_folder = os.path.join(base_input, folder_name)
    os.makedirs(output_root, exist_ok=True)

    print(f"\n>>> 正在处理时段：{folder_name} ，阈值列表：{thresholds}")
    process_all_noise_tifs(input_folder, output_root, thresholds)

print("\n🎉 全部时段处理完成！")