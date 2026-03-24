from osgeo import gdal, ogr
import os
import re
import gc
import uuid

# ========== 配置区 ==========
VECTOR_DICT = {
    0: r"F:\机场噪音\GAUL_2024_调整\均值噪音影响区域\L0_Affected_Areas.shp",
    2: r"F:\机场噪音\GAUL_2024_调整\均值噪音影响区域\L2_Affected_Areas.shp"
}
POP_DIR = r"F:\WordPop\全球人口1KM"
NOISE_BASE_DIR = r"F:\机场噪音\全球噪音_月均值" 
GDP_PATH = r"F:\机场噪音\GDP\GDP_2020_30arcsec.tif"
OUTPUT_ROOT = r"F:\机场噪音\栅格计算"

TARGET_SRS = "EPSG:3857"
RESOLUTION = 1000.0

# 强制开启异常抛出，便于捕获处理
gdal.UseExceptions()

def warp_data_robust(in_path, out_path, shp_path, where_sql):
    if os.path.exists(out_path): return

    # 尝试 1：常规 Warp
    try:
        gdal.SetConfigOption('GDAL_WARP_IGNORE_BAD_CUTLINE', 'YES')
        # 增加内存限制配置，防止俄罗斯这种要素直接撑爆
        warp_options = {
            'dstSRS': TARGET_SRS, 'xRes': RESOLUTION, 'yRes': RESOLUTION,
            'targetAlignedPixels': True, 'cutlineDSName': shp_path, 'cutlineWhere': where_sql,
            'cropToCutline': True, 'resampleAlg': 'bilinear', 'dstNodata': -1,
            'warpMemoryLimit': 500 # 限制内存使用
        }
        ds = gdal.Warp(out_path, in_path, options=gdal.WarpOptions(**warp_options))
        if ds:
            ds = None
            return 
    except Exception:
        if os.path.exists(out_path): os.remove(out_path)

    # 尝试 2：强力拓扑修复与大幅度简化 (针对 Indonesia/Russia)
    tmp_mask = f"tmp_{uuid.uuid4().hex}.json"
    ds_src = None
    try:
        ds_src = ogr.Open(shp_path)
        lyr = ds_src.GetLayer()
        lyr.SetAttributeFilter(where_sql)
        feat = lyr.GetNextFeature()
        
        if feat:
            geom = feat.GetGeometryRef()
            if geom:
                # --- 核心改进：解决自相交与内存溢出 ---
                # 1. 修复自相交 (Buffer(0) 通常比 MakeValid 在跨经度线时更稳)
                repaired_geom = geom.Buffer(0)
                
                # 2. 针对俄罗斯等超大要素，强制简化
                # 1000m分辨率下，简化阈值设为 0.001 (约100米) 完全不影响结果
                point_count = repaired_geom.GetPointCount()
                if point_count == 0: # 说明是 MultiPolygon，需要单独处理
                    point_count = sum([repaired_geom.GetGeometryRef(i).GetPointCount() for i in range(repaired_geom.GetGeometryCount())])
                
                if point_count > 20000:
                    # 动态简化：点数越多，简化力度越大
                    tolerance = 0.001 if point_count < 100000 else 0.005
                    repaired_geom = repaired_geom.Simplify(tolerance)
                
                # 3. 跨经度线处理 (针对 Indonesia/Fiji 等)
                # 强制将所有坐标点规范化，防止环绕 180 度导致的错误
                # 这里我们写入物理文件时，GDAL 会自动处理部分拓扑关系
                driver = ogr.GetDriverByName("GeoJSON")
                tmp_ds = driver.CreateDataSource(tmp_mask)
                tmp_lyr = tmp_ds.CreateLayer("mask", srs=lyr.GetSpatialRef())
                new_feat = ogr.Feature(tmp_lyr.GetLayerDefn())
                new_feat.SetGeometry(repaired_geom)
                tmp_lyr.CreateFeature(new_feat)
                new_feat = None
                tmp_ds = None 

                # 4. 执行 Warp，增加内存错误容错
                opt_repair = gdal.WarpOptions(
                    dstSRS=TARGET_SRS, xRes=RESOLUTION, yRes=RESOLUTION,
                    targetAlignedPixels=True, cutlineDSName=tmp_mask,
                    cropToCutline=True, resampleAlg='bilinear', dstNodata=-1,
                    multithread=False # 超大要素关闭多线程，降低内存峰值
                )
                ds_repair = gdal.Warp(out_path, in_path, options=opt_repair)
                ds_repair = None
    except Exception as e:
        print(f"❌ 终极裁切失败 [{where_sql}]: {e}")
    finally:
        ds_src = None
        if os.path.exists(tmp_mask):
            try: os.remove(tmp_mask)
            except: pass

def get_region_list(shp_path, level):
    """预加载区域信息"""
    ds = ogr.Open(shp_path)
    layer = ds.GetLayer()
    regions = []
    for feat in layer:
        g0 = feat.GetField("gaul0_name") or "Unknown"
        s0 = g0.replace("'", "''") 
        if level == 0:
            name, where = g0, f"gaul0_name = '{s0}'"
        else:
            g1, g2 = feat.GetField("gaul1_name") or "Unknown", feat.GetField("gaul2_name") or "Unknown"
            s1, s2 = g1.replace("'", "''"), g2.replace("'", "''")
            name, where = f"{g0}_{g1}_{g2}", f"gaul0_name = '{s0}' AND gaul1_name = '{s1}' AND gaul2_name = '{s2}'"
        
        safe_name = "".join([c if c.isalnum() or c in "_-" else "_" for c in name])
        regions.append({"id": safe_name, "sql": where})
    return regions

def main():
    gdal.SetCacheMax(512)
    
    # 1. 预加载所有矢量区域（关键优化：不再重复读写大 SHP 文件）
    print("📋 正在预加载矢量区域信息...")
    region_cache = {
        0: get_region_list(VECTOR_DICT[0], 0),
        2: get_region_list(VECTOR_DICT[2], 2)
    }

    # 2. 处理 GDP
    print("🏗️ 正在顺序处理 GDP (L0 & L2)...")
    for level in [0, 2]:
        dest_dir = os.path.join(OUTPUT_ROOT, "GDP", "Static", f"L{level}")
        os.makedirs(dest_dir, exist_ok=True)
        for i, reg in enumerate(region_cache[level]):
            warp_data_robust(GDP_PATH, os.path.join(dest_dir, f"{reg['id']}_GDP_2020.tif"), VECTOR_DICT[level], reg['sql'])
            if i % 100 == 0: gc.collect()

    # 3. 处理 Noise & POP
    sub_modes = ["all", "night", "oneday"]
    for mode in sub_modes:
        search_path = os.path.join(NOISE_BASE_DIR, "all") if mode == "all" else os.path.join(NOISE_BASE_DIR, mode)
        if not os.path.exists(search_path): continue
        
        for root, _, files in os.walk(search_path):
            noise_files = [f for f in files if f.lower().endswith(('.tif', '.tiff'))]
            for n_file in noise_files:
                # 提取年份和标签
                year_match = re.search(r"202\d", n_file)
                year = "2023" if ("all" in root.lower() or not year_match) else year_match.group(0)
                mode_label = "All_Mode" if ("all" in root.lower() or not year_match) else mode
                
                pop_file = next((f for f in os.listdir(POP_DIR) if year in f and f.endswith('.tif')), None)
                inputs = {"Noise": os.path.join(root, n_file)}
                if pop_file: inputs["POP"] = os.path.join(POP_DIR, pop_file)

                # 开始按 Level 裁剪
                for level in [0, 2]:
                    shp_path = VECTOR_DICT[level]
                    regions = region_cache[level]
                    
                    print(f"🚀 正在处理: {n_file} | Level: {level} | 总数: {len(regions)}")
                    
                    for i, reg in enumerate(regions):
                        for d_type, in_p in inputs.items():
                            dest_dir = os.path.join(OUTPUT_ROOT, d_type, f"{year}_{mode_label}", f"L{level}")
                            os.makedirs(dest_dir, exist_ok=True)
                            out_name = f"{reg['id']}_{os.path.basename(in_p)}"
                            warp_data_robust(in_p, os.path.join(dest_dir, out_name), shp_path, reg['sql'])
                        
                        if i % 200 == 0: 
                            print(f"  进度: {i}/{len(regions)}")
                            gc.collect()

if __name__ == "__main__":
    main()