import rasterio
from rasterio.windows import Window
import numpy as np
import os
import warnings

def merge_tifs_by_max_windowed(tif_paths, output_path, tile_size=2048):
    """
    分块读取多个 TIF 文件，取最大值（忽略 NaN/NoData），解决内存溢出问题。
    """
    if not tif_paths:
        return

    # 1. 打开所有源文件
    srcs = [rasterio.open(p) for p in tif_paths]
    
    # 以第一个文件为基准获取元数据
    first_src = srcs[0]
    meta = first_src.meta.copy()
    nodata = first_src.nodata
    height = first_src.height
    width = first_src.width

    # 更新输出元数据：确保分块写入开启，并开启 LZW 压缩
    meta.update({
        'count': 1,
        'tiled': True,
        'blockxsize': 256,
        'blockysize': 256,
        'compress': 'lzw'
    })

    print(f"🚀 开始分块处理大影像: {width}x{height}")
    print(f"🔍 检测到原始 NoData 值为: {nodata}")

    # 忽略 nanmax 在处理全 NaN 块时可能弹出的警告
    warnings.filterwarnings('ignore', category=RuntimeWarning, message='All-NaN slice encountered')

    with rasterio.open(output_path, 'w', **meta) as dst:
        # 2. 循环遍历所有窗口（块）
        for row in range(0, height, tile_size):
            for col in range(0, width, tile_size):
                # 定义当前的窗口范围
                window = Window(col, row, 
                                min(tile_size, width - col), 
                                min(tile_size, height - row))
                
                data_stack = []
                for s in srcs:
                    # 读取数据并转换为 float32 以便支持 NaN 处理
                    block = s.read(1, window=window).astype(np.float32)
                    
                    # 统一将定义的 NoData 转换为标准的 np.nan
                    if nodata is not None and not np.isnan(nodata):
                        block[block == nodata] = np.nan
                    data_stack.append(block)
                
                # 将数据栈沿新维度堆叠 [num_files, height, width]
                stacked = np.stack(data_stack)

                # 3. 计算该块的最大值 (忽略 NaN)
                # 如果某个位置所有图层都是 NaN，结果会是 NaN
                max_block = np.nanmax(stacked, axis=0)
                
                # 4. 还原 NoData 值（如果原始 meta 要求非 NaN 的特定值）
                if nodata is not None and not np.isnan(nodata):
                    max_block[np.isnan(max_block)] = nodata
                
                # 5. 写入目标文件的对应位置
                dst.write(max_block.astype(meta['dtype']), 1, window=window)
                
            percent = (row + tile_size) if (row + tile_size) < height else height
            print(f"  进度: {percent / height * 100:.1f}% ({percent}/{height} 行)")

    # 关闭所有文件流
    for s in srcs:
        s.close()
    
    print(f"✅ 任务完成！结果已保存至: {output_path}")

# === 配置参数 ===
input_folder = r"F:\机场噪音\全球噪音_新\oneday"
tifs = [
    os.path.join(input_folder, "SEL_oneday_202110.tiff"),
    os.path.join(input_folder, "SEL_oneday_202210.tiff"),
    os.path.join(input_folder, "SEL_oneday_202310.tiff")
]
output_file = r"F:\机场噪音\全球噪音_新\all\SEL_oneday1.tiff"

if __name__ == "__main__":
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    merge_tifs_by_max_windowed(tifs, output_file)