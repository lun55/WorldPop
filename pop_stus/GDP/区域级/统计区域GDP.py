"""
统计 GAUL 行政区 GDP（2015）
支持 L0 / L1 / L2
"""
import os
import geopandas as gpd
import pandas as pd
import rasterio
from rasterstats import zonal_stats

# ---------------- 用户配置 ----------------
LEVEL = 1
VECTOR_DICT = {
    0: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L0\GAUL_2024_L0.shp",
    1: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L1\GAUL_2024_L1.shp",
    2: r"F:\机场噪音\GAUL_2024_调整\GAUL_2024_L2\GAUL_2024_L2.shp"
}

GDP_TIF  = r"F:\机场噪音\GDP\GDP_2020_30arcsec.tif"
OUT_CSV  = r"F:\机场噪音\GDP\L{}_GDP_2020.csv".format(LEVEL)
# ------------------------------------------

print(f"统计 L{LEVEL} 级行政区 GDP (2020) ...")

# 读取矢量
gdf = gpd.read_file(VECTOR_DICT[LEVEL]).to_crs("EPSG:4326")

name_cols = (
    ["gaul0_name"] if LEVEL == 0 else
    ["gaul0_name", "gaul1_name"] if LEVEL == 1 else
    ["gaul0_name", "gaul1_name", "gaul2_name"]
)

# 读取 NoData
with rasterio.open(GDP_TIF) as src:
    nodata = src.nodata
    print("GDP NoData =", nodata)

# 区域统计
stats = zonal_stats(
    gdf,
    GDP_TIF,
    stats="sum",
    nodata=nodata,
    all_touched=False
)

# 输出
records = []
for i, row in gdf.iterrows():
    total = stats[i]["sum"]
    rec = {c: row[c] for c in name_cols}
    rec["total_gdp"] = float(total) if total is not None else 0.0
    records.append(rec)

pd.DataFrame(records).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
print("✓ 输出完成：", OUT_CSV)
