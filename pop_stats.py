import os
import geopandas as gpd
from rasterstats import zonal_stats
import pandas as pd
from tqdm import tqdm

# ======== 配置 ========
vector_path = "USA\\gadm41_USA_2.shp"  # 替换为你的市级边界文件路径
raster_folder = r"E:\WordPop\both"
output_folder = r"E:\WordPop\results"

os.makedirs(output_folder, exist_ok=True)

# ======== 读取矢量边界 ========
print("Loading county boundaries...")
counties = gpd.read_file(vector_path)
counties = counties.to_crs(epsg=4326)  # 确保一致

# ======== 扫描栅格文件 ========
tif_files = [os.path.join(raster_folder, f) for f in os.listdir(raster_folder) if f.endswith(".tif")]
print(f"Found {len(tif_files)} raster files.")

# ======== 循环计算 ========
for tif_path in tqdm(tif_files, desc="Processing rasters"):
    filename = os.path.basename(tif_path)
    # 提取年龄段，如 usa_t_05_2025_CN_100m_R2025A_v1.tif → age_group = "05"
    age_group = filename.split("_")[2]
    
    # 计算每个城市的总人口
    stats = zonal_stats(
        vectors=counties,
        raster=tif_path,
        stats=["sum"],
        all_touched=True,  # 边界触及像元也计算
        nodata=-99999 # 无效值
    )
    
    # 整理输出
    df = counties[["GID_2", "NAME_2", "NAME_1"]].copy()
    df["population_sum"] = [s["sum"] for s in stats]
    
    output_csv = os.path.join(output_folder, f"population_age_{age_group}.csv")
    df.to_csv(output_csv, index=False)
    print(f"✅ Saved: {output_csv}")

print("All done!")
