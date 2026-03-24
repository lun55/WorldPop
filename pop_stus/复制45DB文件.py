import os
import shutil
import re

# ========== 用户配置区 ==========
# 源根目录
src_root = r"F:\机场噪音\Noise_Population_Stats_5db_intervals"
dst_root = r"F:\机场噪音\Noise_Population_Stats_HighDB_Filtered"

# 筛选关键词：这里定义什么是“大于45dB”的文件名特征
# 根据你的文件名规律，45dB及以上的区间通常包含以下字符串
high_db_patterns = [
    "45_50dB", "50_55dB", "55_60dB", "60_65dB", "65_70dB", "70_75dB"
]
# ==============================

def filter_and_copy():
    count = 0
    print(f"🚀 开始扫描目录: {src_root}")
    
    # walk 递归遍历所有子文件夹
    for root, dirs, files in os.walk(src_root):
        for file in files:
            # 检查文件名是否包含任何大于45dB的关键词
            if any(pattern in file for pattern in high_db_patterns):
                # 获取文件的绝对路径
                src_file_path = os.path.join(root, file)
                
                # 计算相对路径，以便在目标文件夹中保持结构一致
                rel_path = os.path.relpath(root, src_root)
                target_dir = os.path.join(dst_root, rel_path)
                
                # 创建目标子目录（如果不存在）
                if not os.path.exists(target_dir):
                    os.makedirs(target_dir)
                
                # 执行复制
                dst_file_path = os.path.join(target_dir, file)
                shutil.copy2(src_file_path, dst_file_path) # copy2 保留元数据
                
                print(f"✅ 已复制: {file}")
                count += 1

    print(f"\n🎉 处理完成！")
    print(f"统计：共复制了 {count} 个大于 45dB 的文件。")
    print(f"输出目录: {dst_root}")

if __name__ == "__main__":
    filter_and_copy()