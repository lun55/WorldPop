import os
import geopandas as gpd
from tqdm import tqdm

# 输入文件夹
input_folder = "./USA/split"

# 输出文件夹
output_folder = "./USA/split3857"
os.makedirs(output_folder, exist_ok=True)

# 目标投影
dst_crs = "EPSG:3857"

# 遍历所有子文件夹
for root, dirs, files in os.walk(input_folder):

    for filename in files:
        if not filename.lower().endswith(".shp"):
            continue

        src_path = os.path.join(root, filename)

        # 保持与输入相同的子目录结构
        rel_path = os.path.relpath(root, input_folder)
        out_dir = os.path.join(output_folder, rel_path)
        os.makedirs(out_dir, exist_ok=True)

        # 输出文件路径
        out_name = filename.replace(".shp", ".shp")
        dst_path = os.path.join(out_dir, out_name)
        print(f"Processing: {src_path}")

        # 读取矢量数据
        gdf = gpd.read_file(src_path)

        # 重投影
        gdf_3857 = gdf.to_crs(dst_crs)

        # 写出转换后的矢量文件
        gdf_3857.to_file(dst_path, encoding="utf-8")

        print(f"Saved: {dst_path}")

print("全部 SHP 投影到 EPSG:3857 完成 ✔")
