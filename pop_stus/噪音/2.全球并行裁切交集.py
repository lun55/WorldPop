import os
import numpy as np
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc

# ---------------- 用户配置 ----------------
VECTOR_DICT = {
    0: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L0\GAUL_2024_L0.shp",
    1: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L1\GAUL_2024_L1.shp",
    2: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L2\GAUL_2024_L2.shp"
}

NOISE_SHP_ROOT = r"F:\机场噪音\Vector_Results_New\ALL_40_45"
OUT_BASE_ROOT  = r"F:\机场噪音\County_Noise_Masks_New\ALL_40_45"
MAX_WORKERS    = 8 

# 定义子进程内的全局变量
GDF_BASE_GLOBAL = None

def init_worker(gdf):
    """子进程初始化函数：将基础矢量存入子进程的全局内存"""
    global GDF_BASE_GLOBAL
    GDF_BASE_GLOBAL = gdf

# ---------------- 核心叠加函数 ----------------

def fast_intersect_process(noise_path, out_path, current_keep_cols):
    """执行空间叠加计算"""
    global GDF_BASE_GLOBAL
    try:
        # 增加防御性检查
        if GDF_BASE_GLOBAL is None:
            return "Error: GDF_BASE not initialized"

        noise_gdf = gpd.read_file(noise_path)
        if noise_gdf.empty:
            return False

        # 使用子进程内存中的 GDF_BASE_GLOBAL
        possible_ids = list(GDF_BASE_GLOBAL.sindex.intersection(noise_gdf.total_bounds))
        if not possible_ids:
            with open(out_path + ".empty", "w") as f: f.write("no intersection")
            return True
            
        subset_base = GDF_BASE_GLOBAL.iloc[possible_ids].copy()
        intersected = gpd.overlay(subset_base, noise_gdf, how='intersection')
        
        if intersected.empty:
            with open(out_path + ".empty", "w") as f: f.write("no intersection")
            return True

        final_res = intersected.dissolve(by=current_keep_cols, as_index=False)
        final_res = final_res[current_keep_cols + ['geometry']]
        final_res.to_file(out_path, driver='ESRI Shapefile', encoding='utf-8')
        return True

    except Exception as e:
        return f"Error processing {os.path.basename(noise_path)}: {e}"

# ---------------- 主程序入口 ----------------

if __name__ == '__main__':
    for current_level in [0]:
        print(f"\n" + "="*50)
        print(f"🌍 正在处理行政层级: LEVEL {current_level}")
        print("="*50)

        OUT_ROOT = os.path.join(OUT_BASE_ROOT, f"L{current_level}")
        os.makedirs(OUT_ROOT, exist_ok=True)

        if current_level == 0: 
            KEEP_COLS = ["gaul0_name"]
        elif current_level == 1: 
            KEEP_COLS = ["gaul0_name", "gaul1_name", "gaul1_code"]
        else: 
            KEEP_COLS = ["gaul0_name", "gaul1_name", "gaul2_name", "gaul1_code", "gaul2_code"]

        print(f"📦 正在加载层级 L{current_level} 矢量至内存...")
        gdf_to_share = gpd.read_file(VECTOR_DICT[current_level]).to_crs("EPSG:4326")
        
        tasks = []
        for root, _, files in os.walk(NOISE_SHP_ROOT):
            group_name = os.path.basename(root)
            if group_name == os.path.basename(NOISE_SHP_ROOT): continue
            
            out_dir = os.path.join(OUT_ROOT, group_name)
            os.makedirs(out_dir, exist_ok=True)
            
            for shp in files:
                if shp.lower().endswith(".shp"):
                    in_shp = os.path.join(root, shp)
                    out_shp = os.path.join(out_dir, f"L{current_level}_intersected_{shp}")
                    if not os.path.exists(out_shp) and not os.path.exists(out_shp + ".empty"):
                        tasks.append((in_shp, out_shp))

        print(f"📊 待处理任务总数: {len(tasks)}")

        if tasks:
            # 关键修改：使用 initializer 将 gdf 传给每个子进程
            with ProcessPoolExecutor(
                max_workers=MAX_WORKERS,
                initializer=init_worker,
                initargs=(gdf_to_share,)
            ) as executor:
                future_to_shp = {
                    executor.submit(fast_intersect_process, t[0], t[1], KEEP_COLS): t[0] 
                    for t in tasks
                }
                
                for future in tqdm(as_completed(future_to_shp), total=len(tasks), desc=f"L{current_level} Overlay"):
                    res = future.result()
                    if isinstance(res, str) and "Error" in res:
                        print(f"\n{res}")
        
        del gdf_to_share
        gc.collect()

    print("\n🎉 全部任务完成！")