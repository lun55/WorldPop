import os, re, pandas as pd, geopandas as gpd
from rasterstats import zonal_stats
from tqdm import tqdm

# ---------------- 用户配置 ----------------
LEVEL = 2                  
MASK_ROOT  = rf"F:\机场噪音\PM2.5\County_PM2.5_Masks_New\L{LEVEL}"
POP_ROOT   = r"F:\机场噪音\全球人口1KM"
OUT_ROOT   = rf"F:\机场噪音\PM2.5\PM2.5_人口统计\L{LEVEL}"
os.makedirs(OUT_ROOT, exist_ok=True)

# --- 新增：默认配置 ---
DEFAULT_YEAR = "2022"  # 如果文件名没年份，默认用哪一年的人口
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
print(f"检测到pop_map年份: {list(pop_map.keys())}")
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
        
        pop_tif = pop_map.get(DEFAULT_YEAR)
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
            
            # 从文件名提取关键元数据 (支持 L0_intersected_..._0_5ugm3.shp)
            # 改进正则：捕获末尾的数字范围
            pm_match = re.search(r"_(\d+)_(\d+)ugm3", shp_name)
            if pm_match:
                lower_bound = pm_match.group(1)
                upper_bound = pm_match.group(2)
                pm_range = f"{lower_bound}_{upper_bound}"
            else:
                pm_range = "unknown"
                        
            res = {col: row[col] for col in KEEP_COLS}
            res["year_used"] = DEFAULT_YEAR 
            res["PM_range"] = pm_range
            
            # 获取 zonal_stats 结果
            val = stats[i]["sum"]
            res["affected_pop"] = val if val is not None else 0
            
            # ✅ 关键点：这一行必须存在，否则 results 永远为空
            results.append(res)
        
        # 5. 保存结果
        if results:
            pd.DataFrame(results).to_csv(out_csv, index=False)
        else:
            print(f"⚠️ 警告: {shp_name} 未产生有效统计数据")

print("\n🎉 统计完成！结果已区分昼夜类型并标注人口年份。")