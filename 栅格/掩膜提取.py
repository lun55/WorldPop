from osgeo import gdal, ogr, osr
import os
import re
import gc
import uuid
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

# ========== 配置区 ==========
VECTOR_DICT = {
    0: r"H:\机场噪音\GAUL_2024_调整\20260324\均值噪音影响区域\L0_Affected_Areas.shp",
    2: r"H:\机场噪音\GAUL_2024_调整\20260324\均值噪音影响区域\L2_Affected_Areas.shp"
}
POP_DIR = r"H:\WordPop\全球人口1KM"
NOISE_BASE_DIR = r"H:\机场噪音\全球噪音_月均值\20250324" 
GDP_PATH = r"H:\机场噪音\GDP\GDP_2020_30arcsec.tif"
OUTPUT_ROOT = r"H:\机场噪音\栅格计算\20260324"

TARGET_SRS = "EPSG:3857"
RESOLUTION = 1000.0
MAX_WORKERS = 20  # 根据 CPU 核心数调整

gdal.UseExceptions()

def warp_data_robust(in_path, out_path, shp_path, where_sql):
    """
    强制物理尺寸一致的裁剪函数
    """
    if os.path.exists(out_path): return True

    tmp_mask = f"tmp_{uuid.uuid4().hex}.json"
    try:
        gdal.SetConfigOption('GDAL_WARP_IGNORE_BAD_CUTLINE', 'YES')
        gdal.SetConfigOption('OGR_GEOJSON_MAX_OBJ_SIZE', '0') 
        
        # 1. 获取几何体并修复
        ds_src = ogr.Open(shp_path)
        lyr = ds_src.GetLayer()
        lyr.SetAttributeFilter(where_sql)
        feat = lyr.GetNextFeature()
        if not feat: return False
        
        geom = feat.GetGeometryRef()
        repaired_geom = geom.Buffer(0)
        
        # 物理切割 180 度线 (解决跨日界线拉伸)
        clip_box_wkt = "POLYGON((-179.99 -89.99, 179.99 -89.99, 179.99 89.99, -179.99 89.99, -179.99 -89.99))"
        clip_box = ogr.CreateGeometryFromWkt(clip_box_wkt)
        final_geom = repaired_geom.Simplify(0.0005).Intersection(clip_box)
        if final_geom is None or final_geom.IsEmpty():
            final_geom = repaired_geom

        # 2. 计算强制对齐的边界 (Output Bounds)
        target_srs = osr.SpatialReference()
        target_srs.ImportFromEPSG(3857)
        source_srs = lyr.GetSpatialRef()
        transform = osr.CoordinateTransformation(source_srs, target_srs)
        
        env = final_geom.GetEnvelope() # (minX, maxX, minY, maxY)
        p1 = transform.TransformPoint(env[0], env[2]) # 左下
        p2 = transform.TransformPoint(env[1], env[3]) # 右上
        
        # 核心：像素边缘对齐到 RESOLUTION 的整数倍
        min_x = np.floor(p1[0] / RESOLUTION) * RESOLUTION
        min_y = np.floor(p1[1] / RESOLUTION) * RESOLUTION
        max_x = np.ceil(p2[0] / RESOLUTION) * RESOLUTION
        max_y = np.ceil(p2[1] / RESOLUTION) * RESOLUTION
        
        # 计算该范围下确定的行列数
        out_width = int(round((max_x - min_x) / RESOLUTION))
        out_height = int(round((max_y - min_y) / RESOLUTION))
        aligned_bounds = [min_x, min_y, max_x, max_y]

        # 3. 写入临时 Mask (GeoJSON)
        driver = ogr.GetDriverByName("GeoJSON")
        tmp_ds = driver.CreateDataSource(tmp_mask)
        tmp_lyr = tmp_ds.CreateLayer("mask", srs=source_srs)
        new_feat = ogr.Feature(tmp_lyr.GetLayerDefn())
        new_feat.SetGeometry(final_geom)
        tmp_lyr.CreateFeature(new_feat)
        tmp_ds = None 

        # 4. 执行 Warp
        warp_options = {
            'dstSRS': TARGET_SRS,
            'width': out_width,           # 强制宽度
            'height': out_height,         # 强制高度
            'outputBounds': aligned_bounds, # 强制范围
            'targetAlignedPixels': False, 
            'cutlineDSName': tmp_mask, 
            'cropToCutline': False,       # 关键：禁用自动缩水
            'resampleAlg': 'bilinear', 
            'dstNodata': -1,
            'warpOptions': ['WRAPDATELINE=YES'],
            'warpMemoryLimit': 500
        }
        
        gdal.Warp(out_path, in_path, options=gdal.WarpOptions(**warp_options))
        return True

    except Exception as e:
        print(f"❌ 裁剪失败 [{where_sql}]: {e}")
        return False
    finally:
        if os.path.exists(tmp_mask):
            try: os.remove(tmp_mask)
            except: pass

def warp_worker(args):
    return warp_data_robust(*args)

def get_region_list(shp_path, level):
    ds = ogr.Open(shp_path)
    layer = ds.GetLayer()
    regions = []
    for feat in layer:
        g0 = (feat.GetField("gaul0_name") or "Unknown").replace("'", "''")
        if level == 0:
            name, where = g0, f"gaul0_name = '{g0}'"
        else:
            g1 = (feat.GetField("gaul1_name") or "Unknown").replace("'", "''")
            g2 = (feat.GetField("gaul2_name") or "Unknown").replace("'", "''")
            name, where = f"{g0}_{g1}_{g2}", f"gaul0_name = '{g0}' AND gaul1_name = '{g1}' AND gaul2_name = '{g2}'"
        
        safe_name = "".join([c if c.isalnum() or c in "_-" else "_" for c in name])
        regions.append({"id": safe_name, "sql": where})
    return regions

def run_parallel(task_list):
    if not task_list: return
    print(f"🚀 总任务: {len(task_list)}，并发数: {MAX_WORKERS}")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(warp_worker, task): task for task in task_list}
        count = 0
        for future in as_completed(futures):
            count += 1
            if count % 100 == 0: print(f"进度: {count}/{len(task_list)}")
    gc.collect()

def main():
    region_cache = {0: get_region_list(VECTOR_DICT[0], 0), 2: get_region_list(VECTOR_DICT[2], 2)}

    # 1. GDP 任务
    gdp_tasks = []
    for level in [0, 2]:
        dest_dir = os.path.join(OUTPUT_ROOT, "GDP", "2020", f"L{level}")
        os.makedirs(dest_dir, exist_ok=True)
        for reg in region_cache[level]:
            gdp_tasks.append((GDP_PATH, os.path.join(dest_dir, f"{reg['id']}_GDP_2020.tif"), VECTOR_DICT[level], reg['sql']))
    print("🏗️ 处理 GDP...")
    run_parallel(gdp_tasks)

    # 2. POP 任务
    pop_tasks = []
    pop_files = [f for f in os.listdir(POP_DIR) if f.endswith('.tif')]
    for p_file in pop_files:
        year = re.search(r"202\d", p_file).group(0) if re.search(r"202\d", p_file) else "Unknown"
        for level in [0, 2]:
            dest_dir = os.path.join(OUTPUT_ROOT, "POP", year, f"L{level}")
            os.makedirs(dest_dir, exist_ok=True)
            for reg in region_cache[level]:
                pop_tasks.append((os.path.join(POP_DIR, p_file), os.path.join(dest_dir, f"{reg['id']}_{p_file}"), VECTOR_DICT[level], reg['sql']))
    print("👥 处理 POP...")
    run_parallel(pop_tasks)

    # 3. Noise 任务
    noise_tasks = []
    for root, _, files in os.walk(NOISE_BASE_DIR):
        for n_file in [f for f in files if f.lower().endswith(('.tif', '.tiff'))]:
            folder_name = os.path.splitext(n_file)[0]
            for level in [0, 2]:
                dest_dir = os.path.join(OUTPUT_ROOT, "Noise", folder_name, f"L{level}")
                os.makedirs(dest_dir, exist_ok=True)
                for reg in region_cache[level]:
                    noise_tasks.append((os.path.join(root, n_file), os.path.join(dest_dir, f"{reg['id']}_{n_file}"), VECTOR_DICT[level], reg['sql']))
    print("🔊 处理 Noise...")
    run_parallel(noise_tasks)

if __name__ == "__main__":
    main()