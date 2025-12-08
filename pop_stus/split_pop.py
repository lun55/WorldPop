import os
import geopandas as gpd
from shapely.geometry import box
import rasterio
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject, Resampling


'''
    自动扫描目录下所有 TIF 文件，逐个裁切并重投影
    输入影像：EPSG:4326
    输出影像：EPSG:3857，分辨率=100m
    输出目录：clip/<region>/<age>/<shpName>_clip_3857.tif
    ✅ 已处理 NoData，避免负值
'''


# === 目录配置 ===
year = 2022
shapefile_folder = r"USA\split3857"
tif_folder = rf"G:\US\{year}"
output_root = rf"G:\US\{year}\clip"
buffer_distance = 1000   # 扩充 1000m（EPSG:3857）

TARGET_RES = 100   # 输出影像分辨率 = 100 米

# === 扫描目录中的所有 TIF 文件 ===
tif_files = [f for f in os.listdir(tif_folder) if f.lower().endswith(".tif")]

if not tif_files:
    print("❌ 没有找到任何 TIF 文件")
    exit()

print(f"共检测到 {len(tif_files)} 个 TIF 文件，将逐个处理。\n")


# === 遍历所有 TIF ===
for tif_file in tif_files:

    print(f"\n============================")
    print(f"当前处理影像：{tif_file}")
    print(f"============================")

    tif_path = os.path.join(tif_folder, tif_file)

    # === 解析命名 ===
    parts = tif_file.split("_")
    if len(parts) < 4:
        print(f"⚠ 文件命名不符合预期格式（跳过）: {tif_file}")
        continue

    region_code = parts[0]     # usa
    gender_code = parts[1]     # both
    age_code = parts[2]        # 00
    year_code = parts[3]       # 2023

    # 生成输出目录
    tif_output_folder = os.path.join(output_root, region_code, gender_code, age_code)
    os.makedirs(tif_output_folder, exist_ok=True)

    print(f"→ 解析信息：地区={region_code}, 年龄={age_code}, 年份={year_code}")
    print(f"→ 输出目录：{tif_output_folder}")

    # === 遍历所有 SHP ===
    for shp_file in os.listdir(shapefile_folder):

        if not shp_file.endswith(".shp"):
            continue

        shp_path = os.path.join(shapefile_folder, shp_file)
        print(f"\n--- 裁切区域：{shp_file} ---")

        # 1. 读取 SHP（3857）
        gdf = gpd.read_file(shp_path)
        gdf_3857 = gdf.to_crs("EPSG:3857")

        # 求整体 bounding box + buffer
        minx, miny, maxx, maxy = gdf_3857.total_bounds
        rect = box(minx, miny, maxx, maxy)
        rect_buffered = rect.buffer(buffer_distance)

        # 投影到 4326 用于裁切
        rect_4326 = gpd.GeoSeries([rect_buffered], crs="EPSG:3857").to_crs("EPSG:4326").geometry[0]
        geoms_4326 = [rect_4326.__geo_interface__]

        # === 裁切影像（仍是 4326） ===
        with rasterio.open(tif_path) as src:
            src_nodata = src.nodata if src.nodata is not None else -9999

            try:
                out_image, out_transform = mask(
                    src, geoms_4326, crop=True, nodata=src_nodata
                )
            except ValueError:
                print("⚠ 这个区域不在影像覆盖范围内，跳过")
                continue

            clip_meta = src.meta.copy()
            clip_meta.update({
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
                "crs": src.crs,      # EPSG:4326
                "nodata": src_nodata
            })

            # === 重投影到 3857（分辨率 = 100m） ===
            dst_crs = "EPSG:3857"

            transform, width, height = calculate_default_transform(
                clip_meta["crs"], dst_crs,
                clip_meta["width"], clip_meta["height"],
                *rasterio.transform.array_bounds(
                    clip_meta["height"], clip_meta["width"], clip_meta["transform"]
                ),
                resolution=TARGET_RES
            )

            dst_meta = clip_meta.copy()
            dst_meta.update({
                "crs": dst_crs,
                "transform": transform,
                "width": width,
                "height": height,
                "nodata": src_nodata
            })

            region_name = os.path.splitext(shp_file)[0]
            output_tif = os.path.join(tif_output_folder, f"{region_name}_clip_3857.tif")

            with rasterio.open(output_tif, "w", **dst_meta) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=out_image[i - 1],
                        destination=rasterio.band(dst, i),
                        src_transform=out_transform,
                        src_crs=clip_meta["crs"],
                        dst_transform=transform,
                        dst_crs=dst_crs,
                        resampling=Resampling.nearest,  # ✅ 用 nearest 防止人口数据负值
                        src_nodata=src_nodata,
                        dst_nodata=src_nodata
                    )

        print(f"✓ 完成输出：{output_tif}")


print("\n🎉 所有影像处理完成！")
