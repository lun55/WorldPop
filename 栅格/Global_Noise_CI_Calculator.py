import rasterio
import numpy as np
import os
import re
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed

# ========== 配置区 ==========
BASE_DIR = r"H:\机场噪音\栅格计算\20260324"
OUTPUT_STATS_DIR = r"H:\机场噪音\栅格计算\统计结果_20260324\L2"

THRESHOLD_CONFIG = {
    "oneday": [45, 50, 55],
    "night":  [40, 45, 50]
}
NOISE_UPPER_LIMIT = 75 
DEFAULT_POP_YEAR = "2023" 
MAX_WORKERS = 20  # 并发进程数，视内存大小调整

# ============================

def calculate_ci_core(p_arr, n_arr, threshold=None):
    n_capped = np.where(n_arr > NOISE_UPPER_LIMIT, NOISE_UPPER_LIMIT, n_arr)
    if threshold is not None:
        y_eff = np.where(n_capped < threshold, 0, n_capped)
    else:
        y_eff = n_capped
    
    noise_load = p_arr * y_eff
    total_load, pop_total = noise_load.sum(), p_arr.sum()
    
    if total_load == 0 or pop_total == 0:
        return 0.0
        
    h = noise_load / total_load
    w = p_arr / pop_total
    R_mid = np.cumsum(w) - 0.5 * w
    return 2 * np.sum(h * R_mid) - 1

def get_raster_data(path):
    if not os.path.exists(path): return None, None
    try:
        with rasterio.open(path) as src:
            return src.read(1).astype(np.float64).flatten(), src.nodata
    except Exception as e:
        return None, None

def single_region_worker(task_args):
    """
    单个区域的计算工人
    """
    n_path, g_path, p_path, reg_id, scene, year, level, threshold_list = task_args
    
    n_raw, n_nd = get_raster_data(n_path)
    g_raw, g_nd = get_raster_data(g_path)
    p_raw, p_nd = get_raster_data(p_path)
    
    if n_raw is None or g_raw is None or p_raw is None:
        return None

    if not (n_raw.shape == g_raw.shape == p_raw.shape):
        return {"error": f"Shape mismatch in {reg_id}"}

    mask = (
        (n_raw != n_nd) & np.isfinite(n_raw) &
        (g_raw != g_nd) & np.isfinite(g_raw) & (g_raw > 0) &
        (p_raw != p_nd) & np.isfinite(p_raw) & (p_raw > 0)
    )

    if np.sum(mask) < 2:
        return None

    n_valid, g_valid, p_valid = n_raw[mask], g_raw[mask], p_raw[mask]
    sort_idx = np.argsort(g_valid / p_valid)
    p_sorted, n_sorted = p_valid[sort_idx], n_valid[sort_idx]
    
    res = {
        "Level": level,
        "Region": reg_id,
        "Scene": scene,
        "Year": year,
        "Valid_Pixels": int(np.sum(mask)),
        "CI_Base_NoThreshold": round(calculate_ci_core(p_sorted, n_sorted, threshold=None), 4)
    }
    
    for t_val in threshold_list:
        ci_val = calculate_ci_core(p_sorted, n_sorted, threshold=t_val)
        res[f"CI_T{t_val}"] = round(ci_val, 4)
    
    return res

def process_ci():
    os.makedirs(OUTPUT_STATS_DIR, exist_ok=True)
    noise_root = os.path.join(BASE_DIR, "Noise")
    
    scenes = [d for d in os.listdir(noise_root) if os.path.isdir(os.path.join(noise_root, d))]
    
    all_final_results = []

    for scene in scenes:
        # --- 检查是否已存在结果文件 ---
        scene_csv_path = os.path.join(OUTPUT_STATS_DIR, f"CI_Result_{scene}.csv")
        if os.path.exists(scene_csv_path):
            print(f"⏭️ 跳过已存在的场景: {scene}")
            # 读取已有数据以便最后合并总表
            existing_df = pd.read_csv(scene_csv_path)
            all_final_results.extend(existing_df.to_dict(orient='records'))
            continue

        print(f"🚀 正在并发分析场景: {scene}")
        year = re.search(r"202\d", scene).group(0) if re.search(r"202\d", scene) else DEFAULT_POP_YEAR
        mode = "night" if "night" in scene.lower() else "oneday"
        threshold_list = THRESHOLD_CONFIG[mode]
        
        tasks = []
        level = "L2"
        noise_scene_dir = os.path.join(noise_root, scene, level)
        if not os.path.exists(noise_scene_dir): continue
        
        gdp_folder = os.path.join(BASE_DIR, "GDP", "2020", level)
        pop_folder = os.path.join(BASE_DIR, "POP", year, level)
        
        # 预先扫描目录，减少索引开销
        gdp_files = {f.split("_GDP_")[0]: f for f in os.listdir(gdp_folder) if "_GDP_" in f}
        pop_files = {f.split("_global_pop_")[0]: f for f in os.listdir(pop_folder) if "_global_pop_" in f}

        for n_file in os.listdir(noise_scene_dir):
            if not n_file.lower().endswith((".tif", ".tiff")): continue
            reg_id = n_file.split("_SEL_")[0]
            
            gdp_file = gdp_files.get(reg_id)
            pop_file = pop_files.get(reg_id)
            
            if gdp_file and pop_file:
                tasks.append((
                    os.path.join(noise_scene_dir, n_file),
                    os.path.join(gdp_folder, gdp_file),
                    os.path.join(pop_folder, pop_file),
                    reg_id, scene, year, level, threshold_list
                ))

        # 执行并发计算
        scene_results = []
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(single_region_worker, t): t[3] for t in tasks}
            for future in as_completed(futures):
                res = future.result()
                if res:
                    if "error" in res:
                        print(f"❌ {res['error']}")
                    else:
                        scene_results.append(res)
        
        # 保存该场景
        if scene_results:
            df_scene = pd.DataFrame(scene_results)
            df_scene.to_csv(scene_csv_path, index=False)
            all_final_results.extend(scene_results)
            print(f"✅ 场景 {scene} 完成，保存 {len(scene_results)} 条记录")

    # 保存总表
    if all_final_results:
        df_all = pd.DataFrame(all_final_results)
        cols = [c for c in df_all.columns if not c.startswith("CI_")] + [c for c in df_all.columns if c.startswith("CI_")]
        df_all[cols].to_csv(os.path.join(OUTPUT_STATS_DIR, "CI_Global_Summary_Total.csv"), index=False)
        print(f"\n✨ 全部完成！结果存至: {OUTPUT_STATS_DIR}")

if __name__ == "__main__":
    process_ci()