import os
import sys
import numpy as np
from osgeo import gdal
import rasterio
from pathlib import Path

def Image_Compress(path_image, path_out_image):
    # 转为字符串（兼容 pathlib.Path）
    path_image = str(path_image)
    path_out_image = str(path_out_image)
    
    ds = gdal.Open(path_image)
    if ds is None:
        raise RuntimeError(f"无法打开输入文件: {path_image}")
    
    driver = gdal.GetDriverByName('GTiff')
    out_ds = driver.CreateCopy(
        path_out_image,
        ds,
        strict=1,
        callback=Show_Progress,
        options=[
            "COMPRESS=LZW",
            "PREDICTOR=2"
        ]
    )
    if out_ds is None:
        raise RuntimeError(f"CreateCopy 失败: {path_out_image}")
    
    # 显式关闭
    del ds
    del out_ds

def Show_Progress(percent, msg, tag):
    """
    :param percent: 进度，0~1
    :param msg:
    :param tag:
    :return:
    """
    if 0 <= percent * 100 <= 1:
        print("进度：" + "%.2f" % (0 * 100) + "%")
    if 25 <= percent*100 <= 26:
        print("进度：" + "%.2f" % (percent*100) + "%")
    if 50 <= percent*100 <= 51:
        print("进度：" + "%.2f" % (percent*100) + "%")
    if 75 <= percent*100 <= 76:
        print("进度：" + "%.2f" % (percent*100) + "%")
    if 99 <= percent * 100 <= 100:
        print("进度：" + "%.2f" % (1 * 100) + "%")
def main():
    year = 2023
    population_root = Path(rf"F:\wordpop_USA\both\{year}\clip\usa")  # 原始数据根目录
    output_folder = Path(rf"compress\{year}")            # 压缩输出根目录

    # 创建输出根目录（虽然会在子目录中自动创建，但提前确保更安全）
    output_folder.mkdir(parents=True, exist_ok=True)

    # 递归获取所有 .tif 文件
    file_list = list(population_root.rglob("*.tif"))
    if not file_list:
        print("❌ 未在目录中找到任何 .tif 文件！")
        return

    print(f"共找到 {len(file_list)} 个 GeoTIFF 文件，开始压缩...\n")

    # 遍历每个文件
    for file_path in file_list:
        try:
            # 计算相对路径（如 f/00/xxx.tif）
            print(file_path)
            rel_path = file_path.relative_to(population_root)
            output_path = output_folder / rel_path

            # 创建输出子目录
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # 获取原始大小
            orig_size = file_path.stat().st_size

            # 执行压缩
            Image_Compress(file_path, output_path)

            # 获取压缩后大小
            comp_size = output_path.stat().st_size
            ratio = orig_size / comp_size if comp_size > 0 else 0

            print(f"✅ 处理完成: {rel_path}")
            print(f"    原始大小: {orig_size / (1024**2):.2f} MB")
            print(f"    压缩大小: {comp_size / (1024**2):.2f} MB")
            print(f"    压缩比:   {ratio:.2f}:1\n")

        except Exception as e:
            print(f"❌ 处理失败: {file_path}\n    错误: {e}\n")

    print("✅ 所有文件处理完成！")

if __name__ == "__main__":
    # 验证 GDAL 是否可用
    try:
        from osgeo import gdal
    except ImportError:
        print("❌ 未找到 GDAL Python 绑定，请安装：conda install -c conda-forge gdal")
        sys.exit(1)
    
    main()