import os
import geopandas as gpd
from rasterstats import zonal_stats
import pandas as pd
from tqdm import tqdm

# ======== 配置 ========
vector_path = r"USA\gadm41_USA_2.shp" 
base_raster_folder = r"F:\wordpop_USA\both"  # 根目录
output_folder = r"./population"

os.makedirs(output_folder, exist_ok=True)

# ======== 读取矢量边界 ========
print("Loading county boundaries...")
counties = gpd.read_file(vector_path)
counties = counties.to_crs(epsg=4326)

# 手动指定需要处理的年份列表
target_years = ["2021", "2022", "2023"]

# ======== 循环年份处理 ========
for year in target_years:
    year_path = os.path.join(base_raster_folder, str(year))
    
    # 检查该年份文件夹是否存在
    if not os.path.exists(year_path):
        print(f"⚠️ Skip: Folder for {year} not found.")
        continue
    
    # 递归扫描该年份文件夹下所有 tif (包括子文件夹如 fm)
    tif_files = []
    for root, dirs, files in os.walk(year_path):
        for f in files:
            if f.endswith(".tif"):
                tif_files.append(os.path.join(root, f))
    
    if not tif_files:
        continue
        
    print(f"\nProcessing Year: {year} ({len(tif_files)} files found)")
    
    all_results = []  # 用于存储该年份的所有统计行

    for tif_path in tqdm(tif_files, desc=f"Year {year}"):
        filename = os.path.basename(tif_path)
        
        # 解析文件名: usa_f_00_2023_CN_100m_R2025A_v1.tif
        parts = filename.split("_")
        gender = parts[1]      # 'f' 或 'm'
        age_group = parts[2]   # '00', '05' 等
        
        # 计算空间统计
        stats = zonal_stats(
            vectors=counties,
            raster=tif_path,
            stats=["sum"],
            all_touched=False,
            nodata=-99999
        )
        
        # 将结果按行填入列表，并进行字段重命名
        for idx, row in counties.iterrows():
            all_results.append({
                "GID_2": row["GID_2"],
                "Province": row["NAME_1"],
                "City": row["NAME_2"],
                "Gender": gender,
                "Age": age_group,
                "Population": stats[idx]["sum"] or 0 # 对应您要求的 Affected_Pop 逻辑
            })
    
    # 将该年份所有数据转为 DataFrame 并保存
    year_df = pd.DataFrame(all_results)
    output_csv = os.path.join(output_folder, f"population_usa_{year}.csv")
    year_df.to_csv(output_csv, index=False)
    print(f"Saved annual file: {output_csv}")

print("\nAll done!")