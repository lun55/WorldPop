import rasterio
import numpy as np

in_tif  = r"F:\机场噪音\GDP\GDP_PPP_2015_global_30arcsec.tif"
out_tif = r"F:\机场噪音\GDP\GDP_PPP_2015_nodata_global_30arcsec.tif"

MISSING = -9

with rasterio.open(in_tif) as src:
    profile = src.profile.copy()
    data = src.read(1)

    # 1️⃣ 替换 missing_value
    data = data.astype("float32")
    data[data == MISSING] = src.nodata

    # 2️⃣ 写入 nodata
    profile.update(
        nodata=src.nodata,
        dtype="float32"
    )

    with rasterio.open(out_tif, "w", **profile) as dst:
        dst.write(data, 1)

print("✓ missing_value 已成功转为 src.nodata")
