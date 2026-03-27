import pandas as pd
from osgeo import ogr
import os

# ========== 配置区 ==========
VECTOR_DICT = {
    0: r"H:\机场噪音\GAUL_2024_调整\20260324\均值噪音影响区域\L0_Affected_Areas.shp",
    2: r"H:\机场噪音\GAUL_2024_调整\20260324\均值噪音影响区域\L2_Affected_Areas.shp"
}
OUTPUT_MAPPING_PATH = r"H:\机场噪音\栅格计算\统计结果_20260324\Region_ID_Mapping.csv"
# ============================

def generate_mapping():
    all_mappings = []

    for level, shp_path in VECTOR_DICT.items():
        print(f"正在处理 L{level} 映射关系...")
        if not os.path.exists(shp_path):
            print(f"⚠️ 找不到矢量文件: {shp_path}")
            continue

        ds = ogr.Open(shp_path)
        layer = ds.GetLayer()
        
        for feat in layer:
            # 1. 提取原始字段内容
            g0 = feat.GetField("gaul0_name") or "Unknown"
            g1 = feat.GetField("gaul1_name") if level == 2 else None
            g2 = feat.GetField("gaul2_name") if level == 2 else None

            # 2. 模拟裁剪脚本中的拼接和清洗逻辑
            if level == 0:
                original_combined_name = g0
            else:
                g1_val = g1 or "Unknown"
                g2_val = g2 or "Unknown"
                original_combined_name = f"{g0}_{g1_val}_{g2_val}"
            
            # 严格对应裁剪脚本的清洗规则
            # safe_name = "".join([c if c.isalnum() or c in "_-" else "_" for c in name])
            safe_id = "".join([c if c.isalnum() or c in "_-" else "_" for c in original_combined_name])

            # 3. 记录映射信息
            mapping_row = {
                "Level": f"L{level}",
                "Region_ID": safe_id,          # 统计结果 CSV 里的 Region 列
                "gaul0_name": g0,              # 原始国家名
                "gaul1_name": g1 if g1 else "", # 原始一级行政区
                "gaul2_name": g2 if g2 else "", # 原始二级行政区
                "Full_Path_Name": original_combined_name # 拼接未清洗名
            }
            all_mappings.append(mapping_row)
        
        ds = None # 关闭文件

    # 保存为 CSV
    df = pd.DataFrame(all_mappings)
    # 去重（防止同一区域在矢量里有多块要素导致重复行）
    df = df.drop_duplicates(subset=["Level", "Region_ID"])
    
    os.makedirs(os.path.dirname(OUTPUT_MAPPING_PATH), exist_ok=True)
    df.to_csv(OUTPUT_MAPPING_PATH, index=False, encoding='utf-8-sig')
    print(f"✨ 映射表已生成: {OUTPUT_MAPPING_PATH}")

if __name__ == "__main__":
    generate_mapping()