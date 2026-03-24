"""
计算全球 1 km 人口在各 GAUL 行政单元的总人口
支持 L0/L1/L2 三级，输出：Lx_total_pop_{year}.csv
字段：保留对应 gaulx_name 及 total_pop
"""
import os, re, pandas as pd, geopandas as gpd
from rasterstats import zonal_stats
from tqdm import tqdm

# ---------------- 用户配置 ----------------
LEVEL = 1                      # 0=国家, 1=省州, 2=县市
VECTOR_DICT = {
    0: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L0\GAUL_2024_L0.shp",
    1: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L1\GAUL_2024_L1.shp",
    2: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L2\GAUL_2024_L2.shp"
}
POP_ROOT   = r"F:\WordPop\全球人口1KM"
OUT_ROOT   = r"F:\机场噪音\区域总人口"
os.makedirs(OUT_ROOT, exist_ok=True)

YEARS = ["2021", "2022", "2023"]
NODATA = -99999
# ------------------------------------------

vector_path = VECTOR_DICT[LEVEL]
gdf = gpd.read_file(vector_path).to_crs("EPSG:4326")

# 保留字段
name_cols = ["gaul0_name"] if LEVEL == 0 else \
            ["gaul0_name", "gaul1_name"] if LEVEL == 1 else \
            ["gaul0_name", "gaul1_name", "gaul2_name"]

def pop_tifs():
    for root, _, files in os.walk(POP_ROOT):
        for f in files:
            if f.lower().endswith((".tif", ".tiff")):
                yield os.path.join(root, f)

def extract_year(p):
    return re.search(r"20(?:21|22|23)", p).group()

# 年份→人口文件映射
pop_dict = {extract_year(fp): fp for fp in pop_tifs()}
print("发现人口年份：", list(pop_dict.keys()))

for year in YEARS:
    pop_path = pop_dict.get(year)
    if not pop_path:
        print(f"⚠ 无 {year} 人口数据")
        continue
    out_csv = os.path.join(OUT_ROOT, f"L{LEVEL}_total_pop_{year}.csv")

    # 统计
    stats = zonal_stats(gdf, pop_path, stats="sum", nodata=NODATA, all_touched=False)

    records = []
    for idx, row in gdf.iterrows():
        total = stats[idx]["sum"] or 0
        rec = {c: row[c] for c in name_cols}
        rec["total_pop"] = float(total)
        records.append(rec)

    pd.DataFrame(records).to_csv(out_csv, index=False)
    print(f"✓ {out_csv}")

print("\nAll done!")