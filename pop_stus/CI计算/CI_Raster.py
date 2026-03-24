import rasterio
import numpy as np
import os

# ========== 用户配置区 ==========
data_dir = r"F:\机场噪音\栅格计算\Beijing"
noise_path = os.path.join(data_dir, "Beijing_SEL_oneday_2021.tif")
gdp_path   = os.path.join(data_dir, "Beijing_GDP.tif")
pop_path   = os.path.join(data_dir, "Beijing_POP2021.tif")

THRESHOLD_NIGHT = 40
THRESHOLD_DAY   = 45
NOISE_UPPER_LIMIT = 75  # 超过此值将被重置为 75
# ==============================

def read_raster_with_info(path, name):
    with rasterio.open(path) as src:
        data = src.read(1).flatten()
        nodata = src.nodata
        meta = {
            "name": name,
            "shape": src.shape,
            "res": src.res,
            "bounds": src.bounds,
            "crs": src.crs
        }
        print(f"--- {name} 空间元数据 ---")
        print(f"  维度: {meta['shape']}, 分辨率: {meta['res']}")
        print(f"  坐标系: {meta['crs']}\n")
        return data, nodata, meta

def run_ci_analysis():
    print("📖 正在加载栅格数据...")
    noise_raw, n_nd, n_meta = read_raster_with_info(noise_path, "噪音 (Noise)")
    gdp_raw, g_nd, g_meta   = read_raster_with_info(gdp_path, "经济 (GDP)")
    pop_raw, p_nd, p_meta   = read_raster_with_info(pop_path, "人口 (POP)")

    # 1. 统一掩码过滤 (注意：这里不再剔除超过 75 的噪音)
    mask = (
        np.isfinite(noise_raw) & (noise_raw != n_nd) & 
        np.isfinite(gdp_raw)   & (gdp_raw != g_nd)   & (gdp_raw > 0) &
        np.isfinite(pop_raw)   & (pop_raw != p_nd)   & (pop_raw > 0)
    )
    
    valid_count = np.sum(mask)
    print(f"🔍 识别到有效对齐像元数: {valid_count}")

    if valid_count < 2:
        print("❌ 错误：有效像元不足。")
        return

    # 提取有效像元
    p = pop_raw[mask].astype(np.float64)
    g_total = gdp_raw[mask].astype(np.float64)
    n_original = noise_raw[mask].astype(np.float64)

    # --- 核心改进：执行上限平滑处理 (Capping) ---
    # 超过 NOISE_UPPER_LIMIT 的值设为上限值本身
    n = np.where(n_original > NOISE_UPPER_LIMIT, NOISE_UPPER_LIMIT, n_original)
    
    print(f"📊 原始数据最大值: {np.max(n_original):.2f} dB")
    print(f"📊 参与计算最大值 (平滑后): {np.max(n):.2f} dB")
    # ----------------------------------------

    # 2. 计算人均 GDP 并排序
    g_per_capita = g_total / p
    sort_idx = np.argsort(g_per_capita)
    p_sorted = p[sort_idx]
    n_sorted = n[sort_idx]
    
    def calculate_core_ci(p_arr, n_arr, threshold=None):
        if threshold is not None:
            y_eff = np.where(n_arr < threshold, 0, n_arr)
        else:
            y_eff = n_arr
            
        noise_load = p_arr * y_eff
        total_load = noise_load.sum()
        pop_total  = p_arr.sum()
        
        if total_load == 0: return 0.0
            
        h = noise_load / total_load
        w = p_arr / pop_total
        R_mid = np.cumsum(w) - 0.5 * w
        return 2 * np.sum(h * R_mid) - 1

    # --- 执行计算 ---
    current_threshold = THRESHOLD_NIGHT if "night" in noise_path.lower() else THRESHOLD_DAY
    ci_with_t = calculate_core_ci(p_sorted, n_sorted, threshold=current_threshold)
    ci_no_t = calculate_core_ci(p_sorted, n_sorted, threshold=None)

    print("\n" + "="*40)
    print(f"📊 像元级 CI 计算结果 (Capping 模式: 上限 {NOISE_UPPER_LIMIT}dB)")
    print(f"有阈值模式 ({current_threshold}dB): {ci_with_t}")
    print(f"无阈值模式 (全暴露): {ci_no_t}")
    print("="*40)

if __name__ == "__main__":
    try:
        run_ci_analysis()
    except Exception as e:
        print(f"❌ 运行失败: {e}")