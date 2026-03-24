import rasterio
from rasterio.features import shapes
from rasterio.windows import Window
import geopandas as gpd
import numpy as np
import os
from shapely.geometry import shape
import fiona


def process_tif_in_chunks(tif_path, output_dir, bins, chunk_size=4096):
    file_base_name = os.path.splitext(os.path.basename(tif_path))[0]
    os.makedirs(output_dir, exist_ok=True)

    # --- 优化点 1: 预检查哪些 bins 需要处理 ---
    active_bins = []
    for start, end in bins:
        range_label = f"{start}_{end}dB"
        output_shp = os.path.join(output_dir, f"{file_base_name}_{range_label}.shp")
        if os.path.exists(output_shp):
            print(f"  ⏭  {range_label} 已存在，从计划中移除。")
        else:
            active_bins.append((start, end))

    if not active_bins:
        print(f"  ✨ 所有区间均已处理完成，跳过此文件。")
        return

    # --- 开始处理剩余的 active_bins ---
    with rasterio.open(tif_path) as src:
        height, width = src.height, src.width
        crs = src.crs
        
        # 只为需要处理的区间准备存储
        temp_features = {f"{s}_{e}dB": [] for s, e in active_bins}

        n_rows = (height + chunk_size - 1) // chunk_size
        n_cols = (width + chunk_size - 1) // chunk_size
        total_blocks = n_rows * n_cols
        print(f"  影像尺寸: {height} x {width}, 待处理区间数: {len(active_bins)}")

        block_idx = 0
        for row in range(0, height, chunk_size):
            row_height = min(chunk_size, height - row)
            for col in range(0, width, chunk_size):
                block_idx += 1
                col_width = min(chunk_size, width - col)
                
                window = Window(col, row, col_width, row_height)
                win_transform = src.window_transform(window)
                chunk = src.read(1, window=window)
                
                # 只遍历还未处理的区间
                for start, end in active_bins:
                    range_label = f"{start}_{end}dB"
                    mask = (chunk >= start) & (chunk < end)
                    if not np.any(mask):
                        continue
                    
                    binary = mask.astype(np.uint8)
                    for geom, val in shapes(binary, mask=binary, transform=win_transform):
                        if val == 1:
                            temp_features[range_label].append({
                                'geometry': shape(geom),
                                'properties': {'dB_range': range_label, 'min_db': start}
                            })
                
                if block_idx % 20 == 0 or block_idx == total_blocks:
                    print(f"    进度: {block_idx}/{total_blocks} 块")

        # 保存阶段
        print("  正在保存新生成的区间矢量...")
        for start, end in active_bins:
            range_label = f"{start}_{end}dB"
            features = temp_features[range_label]
            if not features: continue
            
            output_shp = os.path.join(output_dir, f"{file_base_name}_{range_label}.shp")
            gdf = gpd.GeoDataFrame.from_features(features, crs=crs)
            gdf = gdf.dissolve(by="min_db").explode(index_parts=False).reset_index(drop=True)
            gdf = gdf.to_crs("EPSG:4326")
            gdf.to_file(output_shp, driver="ESRI Shapefile", encoding="utf-8")
            print(f"    ✅ {range_label} 已保存。")


def process_all_noise_tifs(input_folder, output_root, max_val, step=5, chunk_size=4096):
    """
    自动扫描目录下所有TIF，分块处理避免内存爆炸
    """
    tif_files = [f for f in os.listdir(input_folder)
                 if f.lower().endswith(('.tif', '.tiff'))]

    if not tif_files:
        print(f"❌ 文件夹 {input_folder} 未找到 TIF 文件。")
        return

    bins = [(i, i + step) for i in range(0, max_val, step)]

    for tif_name in tif_files:
        print(f"\n开始处理: {tif_name}")
        tif_path = os.path.join(input_folder, tif_name)
        
        file_base_name = os.path.splitext(tif_name)[0]
        tif_output_dir = os.path.join(output_root, file_base_name)
        
        try:
            process_tif_in_chunks(tif_path, tif_output_dir, bins, chunk_size)
        except Exception as e:
            print(f"  ❌ 处理失败: {e}")


# ===============================
# === 配置参数 ===
# ===============================
base_input = r"F:\备份\机场噪音\全球噪音_新"
output_root = r"F:\备份\机场噪音\Vector_Results_5db_intervals"

periods = [
    (r"all\oneday", 70),
    # (r"all\night", 65)
]

for folder_name, max_db in periods:
    input_folder = os.path.join(base_input, folder_name)
    if not os.path.exists(input_folder):
        print(f"⚠️ 路径不存在: {input_folder}")
        continue
        
    print(f"\n>>> 正在处理: {folder_name} (0-{max_db}dB, step=5)")
    process_all_noise_tifs(
        input_folder,
        output_root,
        max_val=max_db,
        step=5,
        chunk_size=2048  # 可根据内存调整，2048/4096/8192
    )

print("\n🎉 全部处理完成！")