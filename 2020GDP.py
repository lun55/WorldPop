import rasterio

tif = r"F:\机场噪音\GDP\rast_gdpTot_1990_2020_30arcsec.tif"

with rasterio.open(tif) as ds:
    print("CRS:", ds.crs)
    print("Transform:", ds.transform)
    print("宽高:", ds.width, ds.height)
    print("波段数:", ds.count)
    print("数据类型:", ds.dtypes)
    print("nodata:", ds.nodata)
    print("压缩方式:", ds.profile.get("compress"))

    print("\n--- 每个波段的描述 ---")
    for i in range(1, ds.count + 1):
        print(i, ds.descriptions[i - 1])
        
import rasterio

src_tif = r"F:\机场噪音\GDP\rast_gdpTot_1990_2020_30arcsec.tif"
out_tif = r"F:\机场噪音\GDP\GDP_2020_30arcsec.tif"

band_id = 7  # gdp_2020

with rasterio.open(src_tif) as src:
    profile = src.profile.copy()
    data = src.read(band_id)

    profile.update(
        count=1
    )

    with rasterio.open(out_tif, "w", **profile) as dst:
        dst.write(data, 1)

print("✓ 已成功导出 2020 年 GDP")
