import os
import rasterio
from rasterio.warp import reproject, Resampling

'''
自动裁剪 & 重采样噪音数据，使其格网与人口数据对齐
输入：
    - population_folder: 已处理的人口数据文件夹（EPSG:3857, 100m）
    - noise_path: 原始噪音大影像（整幅）
输出：
    - 按人口区域裁剪 & 对齐后的噪音数据（分辨率100m）
'''

# === 配置 ===
year = 2023
day = "oneday"
population_folder = rf"F:\wordpop_USA\both\2023\clip\usa\f\00"  # 人口数据
noise_path = rf"F:\机场噪音\SEL_{day}_{year}10_95.tiff"             # 原始大影像
output_folder = rf"./noise/USA_tiles/{year}/{day}/noise_aligned"
os.makedirs(output_folder, exist_ok=True)

# === 扫描人口数据文件 ===
pop_files = [f for f in os.listdir(population_folder) if f.endswith(".tif")]

for pop_file in pop_files:
    pop_path = os.path.join(population_folder, pop_file)
    
    # 输出噪音文件名
    region_name = pop_file.replace("_clip_3857.tif", "")
    out_noise_path = os.path.join(output_folder, f"{region_name}_aligned.tif")
    
    with rasterio.open(pop_path) as pop_src, rasterio.open(noise_path) as noise_src:
        pop_meta = pop_src.meta.copy()
        pop_nodata = pop_src.nodata if pop_src.nodata is not None else -9999
        noise_nodata = noise_src.nodata if noise_src.nodata is not None else -9999

        # 更新输出元信息
        pop_meta.update({
            "dtype": noise_src.dtypes[0],  # 使用噪音数据类型
            "nodata": noise_nodata
        })

        # === 直接重采样噪音影像到人口栅格（完全对齐） ===
        with rasterio.open(out_noise_path, "w", **pop_meta) as dst:
            reproject(
                source=rasterio.band(noise_src, 1),      # 原始噪音影像
                destination=rasterio.band(dst, 1),       # 输出影像
                src_transform=noise_src.transform,
                src_crs=noise_src.crs,
                dst_transform=pop_src.transform,         # 人口影像 transform
                dst_crs=pop_src.crs,                     # 人口影像 CRS
                dst_width=pop_src.width,                 # 人口影像宽高
                dst_height=pop_src.height,
                resampling=Resampling.bilinear,          # 或 nearest
                src_nodata=noise_nodata,
                dst_nodata=noise_nodata
            )

    print(f"✓ 噪音数据已裁剪并对齐输出: {out_noise_path}")

print("\n🎉 所有噪音数据已完成裁剪与对齐！")
