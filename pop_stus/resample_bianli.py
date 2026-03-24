import os
import re
import rasterio
from rasterio.warp import reproject, Resampling
import numpy as np 

# ---------- 配置 ----------
POP_ROOT   = r"F:\WordPop\全球人口1KM"
NOISE_ROOT = r"F:\机场噪音\全球\补"
OUT_ROOT   = r"F:\机场噪音\重采样"          # 输出根目录
os.makedirs(OUT_ROOT, exist_ok=True)

# 支持扩展名
TIF_EXTS = (".tif", ".tiff")

# ---------- 工具函数 ----------
def find_files(folder, exts):
    """递归找所有符合扩展名的文件"""
    for root, _, files in os.walk(folder):
        for f in files:
            if f.lower().endswith(exts):
                yield os.path.join(root, f)

def extract_year(path):
    """从路径提取 4 位年份"""
    m = re.search(r"20(?:21|22|23)", path)
    return m.group() if m else None

# ---------- 主流程 ----------
# 1. 扫描人口影像（按年份分组）
pop_dict = {}   # {year: file_path}
for fp in find_files(POP_ROOT, TIF_EXTS):
    yr = extract_year(fp)
    if yr:
        pop_dict[yr] = fp

# 2. 扫描噪音影像
noise_files = list(find_files(NOISE_ROOT, TIF_EXTS))
print(f"发现人口数据年份：{list(pop_dict.keys())}")
print(f"发现噪音影像数：{len(noise_files)}")

# 3. 逐影像处理
for nfp in noise_files:
    yr = extract_year(nfp)
    if yr not in pop_dict:
        print(f"⚠  跳过（无对应年份人口）: {nfp}")
        continue
    pfp = pop_dict[yr]

    # 输出子目录 & 文件名
    out_dir = os.path.join(OUT_ROOT, yr)
    os.makedirs(out_dir, exist_ok=True)
    base_name = os.path.basename(nfp).split(".")[0] + "_res.tif"
    out_path  = os.path.join(out_dir, base_name)

    # 4. 对齐重采样
    with rasterio.open(pfp) as pop_src, rasterio.open(nfp) as noise_src:
        meta = pop_src.meta.copy()
        meta.update(dtype=noise_src.dtypes[0],
                    nodata=noise_src.nodata if noise_src.nodata is not None else -9999)
         # ===== 新增：先把整块噪音读出来，负值→0 =====
        noise_arr = noise_src.read(1)          # 整幅二维数组
        noise_arr = np.where(noise_arr < 0, 0, noise_arr)  # 负值改0
        # =================================================
        with rasterio.open(out_path, "w", **meta) as dst:
            reproject(
                source=noise_arr,
                destination=rasterio.band(dst, 1),
                src_transform=noise_src.transform,
                src_crs=noise_src.crs,
                dst_transform=pop_src.transform,
                dst_crs=pop_src.crs,
                dst_width=pop_src.width,
                dst_height=pop_src.height,
                resampling=Resampling.bilinear,
                src_nodata=noise_src.nodata,
                dst_nodata=meta["nodata"]
            )
    print(f"✓ {out_path}")

print("\n🎉 全部对齐完成！")