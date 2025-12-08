import os
import geopandas as gpd
import pandas as pd
from rasterstats import zonal_stats
import rasterio
import numpy as np
from tqdm import tqdm

# ================= 配置 =================
year = 2023
day = "oneday"
noise_thresholds = [55, 60, 70]
export_affected_raster = False   # ❗是否输出中间的 tif（默认 False）
# population_root = rf"G:\US\{year}\clip\usa"
population_root = rf"F:\wordpop_USA\both\{year}\clip\usa"
noise_root = rf"./noise/USA_tiles/{year}/{day}/noise_aligned"
shapefile_folder = r"USA\split3857"
output_root = rf"./noise/USA_tiles/{year}/{day}/results"

genders = ["f", "m"]
age_groups = [d for d in os.listdir(os.path.join(population_root, "f"))
              if os.path.isdir(os.path.join(population_root, "f", d))]

os.makedirs(output_root, exist_ok=True)

# ================= 遍历区域 shapefile =================
shp_files = [f for f in os.listdir(shapefile_folder) if f.endswith(".shp")]

for shp_file in tqdm(shp_files, desc="Processing regions"):
    region_name = os.path.splitext(shp_file)[0]
    shp_path = os.path.join(shapefile_folder, shp_file)

    counties = gpd.read_file(shp_path)

    all_results = []

    # ================= 遍历（性别 × 年龄） =================
    for gender in genders:
        for age in age_groups:

            pop_folder = os.path.join(population_root, gender, age)
            pop_path = os.path.join(pop_folder, f"{region_name}_clip_3857.tif")
            noise_path = os.path.join(noise_root, f"{region_name}_aligned.tif")

            if not os.path.exists(pop_path) or not os.path.exists(noise_path):
                continue

            with rasterio.open(pop_path) as pop_src, rasterio.open(noise_path) as noise_src:

                counties = counties.to_crs(pop_src.crs)

                pop_nodata = pop_src.nodata if pop_src.nodata is not None else -9999
                noise_nodata = noise_src.nodata if noise_src.nodata is not None else -9999

                pop_data = pop_src.read(1)
                noise_data = noise_src.read(1)

                # ================= 遍历阈值 =================
                for threshold in noise_thresholds:

                    # 直接生成 numpy 数组
                    affected_pop_data = np.where(
                        (noise_data >= threshold) & (pop_data != pop_nodata),
                        pop_data,
                        0
                    ).astype(pop_data.dtype)

                    # ===== 可选：输出受影响人口 TIFF =====
                    if export_affected_raster:
                        out_tif_folder = os.path.join(output_root, "tif", f"{threshold}dB")
                        os.makedirs(out_tif_folder, exist_ok=True)
                        out_tif_path = os.path.join(out_tif_folder,
                            f"{region_name}_{gender}_{age}_affected_{threshold}dB.tif")

                        meta = pop_src.meta.copy()
                        meta.update({"dtype": affected_pop_data.dtype, "nodata": 0})

                        with rasterio.open(out_tif_path, "w", **meta) as dst:
                            dst.write(affected_pop_data, 1)

                    # ===== 使用 zonal_stats（不需写 tif!!!）=====
                    stats = zonal_stats(
                        vectors=counties,
                        raster=affected_pop_data,         # 直接塞 ndarray
                        affine=pop_src.transform,         # 提供 transform
                        nodata=0,
                        stats=["sum"],
                        all_touched=True
                    )

                    # 整理结果
                    for idx, row in counties.iterrows():
                        all_results.append({
                            "GID_2": row["GID_2"],
                            "Province": row["NAME_1"],
                            "City": row["NAME_2"],
                            "Gender": gender,
                            "Age": age,
                            "Noise_Threshold": threshold,
                            "Affected_Pop": int(stats[idx]["sum"] or 0)
                        })

    # ================= 输出当前 region 的 csv =================
    if all_results:
        df = pd.DataFrame(all_results)
        os.makedirs(os.path.join(output_root, "csv"), exist_ok=True)
        df.to_csv(
            os.path.join(output_root, "csv", f"{region_name}_affected_population.csv"),
            index=False,
            encoding="utf-8-sig"
        )
