import geopandas as gpd
import os
from tqdm import tqdm
from shapely.prepared import prep
from concurrent.futures import ProcessPoolExecutor, as_completed

# 1. 修改任务函数：在子进程内部进行 prep
def check_and_intersect(row_tuple, noise_geom_raw):
    """
    row_tuple: (Index, GID_2, NAME_1, NAME_2, geometry)
    """
    county_geom = row_tuple[4]
    
    # 在子进程中创建 Prepared Geometry
    # 这样可以绕过序列化问题，且每个子进程只需 prep 一次噪声面
    prepared_noise = prep(noise_geom_raw)
    
    if prepared_noise.intersects(county_geom):
        inter_geom = county_geom.intersection(noise_geom_raw)
        
        if not inter_geom.is_empty and inter_geom.geom_type in ['Polygon', 'MultiPolygon']:
            return {
                "GID_2": row_tuple[1],
                "NAME_1": row_tuple[2],
                "NAME_2": row_tuple[3],
                "geometry": inter_geom
            }
    return None

def step2_overlay_parallel(vector_root, counties_shp_path, output_overlay_root, max_workers=None):
    print("正在加载美国县级边界数据...")
    counties_gdf = gpd.read_file(counties_shp_path, engine="pyogrio").to_crs("EPSG:4326")
    counties_gdf = counties_gdf[['GID_2', 'NAME_1', 'NAME_2', 'geometry']]
    counties_sindex = counties_gdf.sindex
    usa_total_bounds = counties_gdf.total_bounds 

    sub_folders = [f for f in os.listdir(vector_root) if os.path.isdir(os.path.join(vector_root, f))]

    for folder in sub_folders:
        print(f"\n处理噪音组: {folder}")
        input_dir = os.path.join(vector_root, folder)
        output_dir = os.path.join(output_overlay_root, folder)
        os.makedirs(output_dir, exist_ok=True)

        noise_shps = [f for f in os.listdir(input_dir) if f.endswith('.shp')]

        for noise_file in tqdm(noise_shps, desc="Overall Progress"):
            output_path = os.path.join(output_dir, f"intersected_{noise_file}")
            if os.path.exists(output_path): continue

            try:
                # 读取并获取单一几何体
                noise_gdf = gpd.read_file(os.path.join(input_dir, noise_file), engine="pyogrio")
                if noise_gdf.crs != counties_gdf.crs:
                    noise_gdf = noise_gdf.to_crs(counties_gdf.crs)
                
                # 合并几何体，只传原始 geom 给子进程
                noise_geom_raw = noise_gdf.geometry.unary_union
                if noise_geom_raw.is_empty: continue

                # 空间索引筛选
                minx, miny, maxx, maxy = noise_geom_raw.bounds
                possible_idx = list(counties_sindex.intersection((minx, miny, maxx, maxy)))
                if not possible_idx: continue
                
                relevant_counties = counties_gdf.iloc[possible_idx]
                
                # 转换为元组列表
                tasks = list(relevant_counties.itertuples(index=True, name=None))
                
                results = []
                # 4. 并行计算
                with ProcessPoolExecutor(max_workers=max_workers) as executor:
                    # 关键修改：只传递原始的 noise_geom_raw
                    futures = [executor.submit(check_and_intersect, task, noise_geom_raw) for task in tasks]
                    
                    for future in tqdm(as_completed(futures), total=len(futures), desc=f"  Parallel -> {noise_file[-15:]}", leave=False):
                        res = future.result()
                        if res:
                            results.append(res)
                
                if results:
                    intersected_gdf = gpd.GeoDataFrame(results, crs=counties_gdf.crs)
                    intersected_gdf.to_file(output_path, engine="pyogrio")
                    
            except Exception as e:
                print(f"  ❌ 出错 {noise_file}: {e}")

if __name__ == "__main__":
    vector_results_root = r"F:\机场噪音\Vector_Results"
    counties_shp = r"USA\gadm41_USA_2.shp"
    overlay_output = r"F:\机场噪音\County_Noise_Masks\美国"
    
    # 建议设置比最大核心数稍微少一点，防止系统卡死
    step2_overlay_parallel(vector_results_root, counties_shp, overlay_output, max_workers=None)