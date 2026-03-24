import xarray as xr
import os
import rasterio
import numpy as np

NC_PATH = r"F:\PM2.5\GHAP_PM2.5_M1K_202210_V1.nc"
OUT_TIF = r"F:\PM2.5\GHAP_PM2.5_M1K_202210.tif"
# OUT_TIF = r"F:/GHAP_PM2.5_M1K_202210.tif"

# 1. 打开数据
ds = xr.open_dataset(NC_PATH)
ds = ds.rename({"lat": "y", "lon": "x"})
pm = ds["PM2.5"].squeeze(drop=True)

# 2. ✅ 处理无效值 (根据作者源码，把 65535 变成 NaN)
# 这样你计算出来的 Max 就不会再出现 3000 多了
pm = pm.where(pm != 65535, np.nan)

# 3. 导出 (rioxarray 比原始 GDAL 稳健得多，不容易报 DLL 错误)
pm.rio.write_crs("EPSG:4326", inplace=True)
pm.rio.to_raster(OUT_TIF, tiled=True, compress="LZW", BIGTIFF="YES")

print(f"✅ 修正后的文件已生成，Max 应该恢复正常了。")

# import rasterio
# import numpy as np

# OUT_TIF = r"F:\机场噪音\PM2.5\GHAP_PM2.5_M1K_202210.tif"

# with rasterio.open(OUT_TIF) as src:
#     # 读取第一波段
#     # masked=True 会自动将 src.nodata 对应的像素转化为掩码，不计入统计
#     data = src.read(1, masked=True)
    
#     # 获取元数据中的 NoData 值
#     nodata = src.nodata
    
#     # 统计信息
#     actual_max = np.ma.max(data)
#     actual_min = np.ma.min(data)
#     actual_mean = np.ma.mean(data)
    
#     # 无效值总数 (被掩码的像素数)
#     invalid_count = np.sum(data.mask)
#     # 有效值总数
#     valid_count = data.count()

# print(f"--- TIF 真实物理统计 ---")
# print(f"最大值: {actual_max}")
# print(f"最小值: {actual_min}")
# print(f"平均值: {actual_mean}")
# print(f"元数据定义的 NoData: {nodata}")
# print(f"有效像素数: {valid_count}")
# print(f"无效像素数: {invalid_count}")