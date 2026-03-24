import os
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
import shutil
import warnings
# 屏蔽 pyogrio 关于 winding order 的警告，让界面保持干净
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*winding order.*")

# ---------------- 用户配置 ----------------
ENGINE = "pyogrio" 

VECTOR_DICT = {
    0: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L0\GAUL_2024_L0.shp",
    1: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L1\GAUL_2024_L1.shp",
    2: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L2\GAUL_2024_L2.shp"
}
NOISE_SHP_ROOT = r"F:\备份\机场噪音\Vector_Results_5db_intervals"
OUT_BASE_ROOT  = r"F:\备份\机场噪音\County_Noise_Masks_5db_intervals"
MAX_WORKERS    = 10

# ---------------- 核心裁剪函数 ----------------

def process_country_task(noise_path, country_gdf, out_path, current_keep_cols):
    """
    单个进程裁切一个国家内部的噪音
    """
    try:
        noise_gdf = gpd.read_file(noise_path, engine=ENGINE)
        if noise_gdf.empty: return False

        intersected = gpd.clip(country_gdf, noise_gdf)
        if intersected.empty: return False

        final_res = intersected.dissolve(by=current_keep_cols, as_index=False)
        final_res = final_res[current_keep_cols + ['geometry']]
        
        final_res.to_file(out_path, engine=ENGINE)
        return True
    except Exception as e:
        return f"Error: {e}"

# ---------------- 主程序 ----------------

if __name__ == '__main__':
    for current_level in [0, 1, 2]:
        print(f"\n" + "="*60)
        print(f"🌍 正在处理行政层级: LEVEL {current_level}")
        print("="*60)

        # 1. 加载行政矢量
        full_gdf = gpd.read_file(VECTOR_DICT[current_level], engine=ENGINE).to_crs("EPSG:4326")
        countries = full_gdf["gaul0_name"].unique()
        
        cols = ["gaul0_name"]
        if current_level >= 1: cols += ["gaul1_name", "gaul1_code"]
        if current_level == 2: cols += ["gaul2_name", "gaul2_code"]

        # 2. 递归遍历噪音目录
        for root, _, files in os.walk(NOISE_SHP_ROOT):
            group_name = os.path.basename(root)
            if group_name == os.path.basename(NOISE_SHP_ROOT): continue
            
            for shp in files:
                if not shp.lower().endswith(".shp"): continue
                
                noise_path = os.path.join(root, shp)
                noise_label = shp.replace(".shp", "")
                
                # --- 严格统一命名规则 ---
                level_dir = os.path.join(OUT_BASE_ROOT, f"L{current_level}", group_name)
                # 目标文件名：L0_intersected_SEL_oneday_0_5dB.shp
                final_merged_shp = os.path.join(level_dir, f"L{current_level}_intersected_{shp}")
                # 临时碎片目录
                temp_out_dir = os.path.join(level_dir, f"temp_{noise_label}")
                
                os.makedirs(level_dir, exist_ok=True)

                # --- 1. 整文件跳过检查 ---
                if os.path.exists(final_merged_shp):
                    # print(f"⏭️  跳过已完成文件: {os.path.basename(final_merged_shp)}")
                    continue

                # 读取噪音四至用于空间预过滤
                n_meta = gpd.read_file(noise_path, engine=ENGINE, rows=0)
                noise_bounds = n_meta.total_bounds

                tasks = []
                os.makedirs(temp_out_dir, exist_ok=True)
                
                for country_name in countries:
                    # 碎片命名：temp_xxx/China.shp
                    out_shp = os.path.join(temp_out_dir, f"{country_name}.shp")
                    empty_mark = os.path.join(temp_out_dir, f"{country_name}.empty")
                    
                    # --- 2. 碎片级跳过检查 ---
                    if os.path.exists(out_shp) or os.path.exists(empty_mark):
                        continue
                    
                    country_subset = full_gdf[full_gdf["gaul0_name"] == country_name].copy()
                    
                    # 空间预过滤 (AABB Test)
                    c_bounds = country_subset.total_bounds
                    if (c_bounds[0] > noise_bounds[2] or c_bounds[2] < noise_bounds[0] or
                        c_bounds[1] > noise_bounds[3] or c_bounds[3] < noise_bounds[1]):
                        with open(empty_mark, "w") as f: f.write("no_overlap")
                        continue

                    tasks.append((noise_path, country_subset, out_shp))

                # 3. 并行计算
                if tasks:
                    print(f"📦 正在处理: {noise_label} ({len(tasks)} 任务)")
                    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [executor.submit(process_country_task, t[0], t[1], t[2], cols) for t in tasks]
                        for _ in tqdm(as_completed(futures), total=len(tasks), desc="Progress"):
                            pass

                # 4. 碎片合并
                fragment_files = [os.path.join(temp_out_dir, f) for f in os.listdir(temp_out_dir) if f.endswith(".shp")]
                
                if fragment_files:
                    print(f"🔗 合并碎片 -> {os.path.basename(final_merged_shp)}")
                    merged_gdf = pd.concat([gpd.read_file(f, engine=ENGINE) for f in fragment_files])
                    merged_gdf.to_file(final_merged_shp, engine=ENGINE)
                    
                    # 清理
                    try:
                        shutil.rmtree(temp_out_dir)
                    except:
                        pass
                else:
                    # 如果没有任何重叠，生成标记避免重复扫描
                    with open(final_merged_shp + ".empty_globe", "w") as f: f.write("none")
                    if os.path.exists(temp_out_dir): shutil.rmtree(temp_out_dir)

        del full_gdf
        gc.collect()

    print("\n🎉 任务全部完成，命名已与原始格式完全对齐。")