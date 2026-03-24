import os, re, pandas as pd, geopandas as gpd
from rasterstats import zonal_stats
from tqdm import tqdm

# ---------------- 用户配置 ----------------
LEVEL = 0                 
MASK_ROOT  = rf"F:\机场噪音\County_Noise_Masks_New\ALL_40_45\L{LEVEL}"
POP_ROOT   = r"F:\机场噪音\全球人口1KM"
OUT_ROOT   = rf"F:\机场噪音\Noise_Population_Stats_New\ALL_40_45L{LEVEL}"
os.makedirs(OUT_ROOT, exist_ok=True)

# --- 新增：默认配置 ---
DEFAULT_YEAR = "2023"  # 如果文件名没年份，默认用哪一年的人口
NODATA = -99999

if LEVEL == 0: 
    KEEP_COLS = ["gaul0_name"]
elif LEVEL == 1: 
    KEEP_COLS = ["gaul0_name", "gaul1_name", "gaul1_code"]
else: 
    KEEP_COLS = ["gaul0_name", "gaul1_name", "gaul2_name", "gaul1_code", "gaul2_code"]

# ---------------- 辅助函数 ----------------

def get_pop_map():
    pop_map = {}
    for root, _, files in os.walk(POP_ROOT):
        for f in files:
            if f.lower().endswith((".tif", ".tiff")):
                match = re.search(r"20\d{2}", f)
                if match:
                    pop_map[match.group()] = os.path.join(root, f)
    return pop_map

# ---------------- 主程序 ----------------

pop_map = get_pop_map()
print(f"检测到人口年份: {list(pop_map.keys())}")
print(f"未识别年份的文件将默认使用: {DEFAULT_YEAR} 年数据")

for group_name in os.listdir(MASK_ROOT):
    group_path = os.path.join(MASK_ROOT, group_name)
    if not os.path.isdir(group_path): continue
    
    group_out_dir = os.path.join(OUT_ROOT, group_name)
    os.makedirs(group_out_dir, exist_ok=True)
    
    # 筛选 shp 文件
    shp_files = [f for f in os.listdir(group_path) if f.lower().endswith(".shp")]
    
    for shp_name in tqdm(shp_files, desc=f"Processing {group_name}"):
        shp_path = os.path.join(group_path, shp_name)
        out_csv = os.path.join(group_out_dir, shp_name.replace(".shp", ".csv"))
        if(os.path.exists(out_csv)):
            continue
        # 1. 识别年份逻辑
        year_match = re.search(r"20\d{2}", shp_name)
        if year_match:
            year = year_match.group()
        else:
            year = DEFAULT_YEAR # 无法识别时使用默认年
        
        pop_tif = pop_map.get(year)
        if not pop_tif:
            continue

        # 2. 加载矢量
        mask_gdf = gpd.read_file(shp_path)
        if mask_gdf.empty: continue

        # 3. Zonal Stats
        # 注意：对于 1km 人口数据，sum 即代表该区域内的总人口数
        stats = zonal_stats(mask_gdf, pop_tif, stats="sum", nodata=NODATA)

        # 4. 组装结果
        results = []
        for i in range(len(mask_gdf)):
            row = mask_gdf.iloc[i]
            
            # 从文件名提取关键元数据
            db_match = re.search(r"(\d+)dB", shp_name)
            db_level = db_match.group(1) if db_match else "unknown"
            
            # 识别是 night 还是 oneday
            time_type = "night" if "night" in shp_name.lower() else "oneday"
            
            res = {col: row[col] for col in KEEP_COLS}
            res["year_used"] = year       # 记录实际使用的是哪年的人口
            res["time_type"] = time_type  # 区分昼夜
            res["dB_level"] = db_level
            res["affected_pop"] = stats[i]["sum"] if stats[i]["sum"] is not None else 0
            results.append(res)
        
        # 5. 保存结果
        pd.DataFrame(results).to_csv(out_csv, index=False)

print("\n🎉 统计完成！结果已区分昼夜类型并标注人口年份。")