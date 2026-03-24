import rasterio
from rasterio.features import shapes
from rasterio.windows import Window
import geopandas as gpd
import numpy as np
import os
from shapely.geometry import shape

def vectorize_pm25_health_standards(tif_path, output_root, tile_size=4096):
    """
    针对 0-182 范围，采用不等间隔健康阈值进行分块矢量化
    """
    if not os.path.exists(tif_path):
        print(f"❌ 未找到文件: {tif_path}")
        return

    # 1. 定义健康阈值区间 (WHO & 中国标准)
    # 区间：0-5, 5-10, 10-15, 15-25, 25-35, 35-50, 50-75, 75-115, 115-150, 150-185
    thresholds = [0, 5, 10, 15, 25, 35, 50, 75, 115, 150, 185]
    bins = [(thresholds[i], thresholds[i+1]) for i in range(len(thresholds)-1)]

    file_base_name = os.path.splitext(os.path.basename(tif_path))[0]
    print(f"🚀 开始处理 PM2.5 影像 (不等间隔健康分级): {file_base_name}")
    
    tif_output_dir = os.path.join(output_root, file_base_name)
    os.makedirs(tif_output_dir, exist_ok=True)

    with rasterio.open(tif_path) as src:
        full_width, full_height = src.width, src.height
        src_crs = src.crs
        # 获取 NoData 值，防止将 65535 误认为浓度
        nodata_val = src.nodata if src.nodata is not None else 65535

        for start, end in bins:
            range_label = f"{start}_{end}ugm3"
            output_shp_path = os.path.join(tif_output_dir, f"{file_base_name}_{range_label}.shp")
            
            if os.path.exists(output_shp_path):
                print(f"  ⏭️ 跳过已存在区间: {range_label}")
                continue

            print(f"  正在提取区间: {range_label}...")
            all_features = []

            # 2. 分块读取逻辑（解决内存溢出）
            for row in range(0, full_height, tile_size):
                for col in range(0, full_width, tile_size):
                    window = Window(col, row, 
                                    min(tile_size, full_width - col), 
                                    min(tile_size, full_height - row))
                    
                    # 读取局部块
                    block = src.read(1, window=window)
                    
                    # 3. 显式二值化 + 排除 NoData (65535) 和异常高值
                    binary_mask = np.zeros(block.shape, dtype=np.uint8)
                    # 严格判定：在区间内且不能等于 NoData
                    condition = (block >= start) & (block < end) & (block != nodata_val)
                    binary_mask[condition] = 1

                    if not np.any(binary_mask):
                        continue

                    # 4. 矢量化局部块
                    win_transform = src.window_transform(window)
                    shape_gen = shapes(
                        binary_mask, 
                        mask=(binary_mask == 1), 
                        transform=win_transform
                    )

                    for geom, val in shape_gen:
                        all_features.append({
                            'properties': {'min_pm': float(start), 'max_pm': float(end)}, 
                            'geometry': shape(geom)
                        })

            # 5. 合并、溶解并保存
            if all_features:
                gdf = gpd.GeoDataFrame(all_features, crs=src_crs)
                # 融合碎片，减少文件体积
                gdf_cleaned = gdf.dissolve().explode(index_parts=False).reset_index(drop=True)

                if gdf_cleaned.crs != "EPSG:4326":
                    gdf_cleaned = gdf_cleaned.to_crs("EPSG:4326")

                gdf_cleaned.to_file(output_shp_path, driver='ESRI Shapefile', encoding='utf-8')
                print(f"    ✅ 成功导出: {range_label}")
            else:
                print(f"    ⚪ 区间 {range_label} 无有效像元")

# === 配置区 ===
pm25_input = r"F:\机场噪音\PM2.5\GHAP_PM2.5_M1K_202210.tif"
pm25_output = r"F:\机场噪音\PM2.5\PM2.5_Vector_Results"

if __name__ == "__main__":
    vectorize_pm25_health_standards(pm25_input, pm25_output)
    print("\n🎉 所有健康分级矢量化任务已完成！")