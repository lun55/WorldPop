import pandas as pd
import os
import glob
import re

# ========== 配置区 ==========
# 1. 映射表路径
MAPPING_CSV = r"H:\机场噪音\栅格计算\统计结果_20260324\Region_ID_Mapping.csv"

# 2. 原始统计结果根目录 (程序会自动向下寻找 L0/L2)
STATS_BASE_DIR = r"H:\机场噪音\栅格计算\统计结果_20260324"

# 3. 挂接后的输出根目录
FINAL_OUTPUT_ROOT = r"H:\机场噪音\栅格计算\统计结果_20260324\CI_Mapping_Results"

# 4. 级别列表
LEVELS = ["L0", "L2"]
# ============================

def merge_ci_by_level():
    # --- 1. 加载映射表 ---
    if not os.path.exists(MAPPING_CSV):
        print(f"❌ 错误：找不到映射表 {MAPPING_CSV}")
        return
    
    # 读取映射表并准备挂接列
    df_map_all = pd.read_csv(MAPPING_CSV)
    df_map_all = df_map_all.rename(columns={"Region_ID": "Region"})
    print(f"✅ 映射表读取成功，共 {len(df_map_all)} 条区域信息。")

    # --- 2. 按级别处理 ---
    for lv in LEVELS:
        print(f"\n📂 正在处理级别: {lv}...")
        
        # 确定该级别下的输出目录
        lv_output_dir = os.path.join(FINAL_OUTPUT_ROOT, lv)
        os.makedirs(lv_output_dir, exist_ok=True)
        
        # 仅筛选映射表中属于当前 Level 的数据，提高挂接效率
        df_map_lv = df_map_all[df_map_all["Level"] == lv].copy()

        # 在统计目录下寻找属于该 Level 的所有 CSV 文件
        # 匹配模式：STATS_BASE_DIR/**/L2/*.csv 或类似结构
        search_pattern = os.path.join(STATS_BASE_DIR, "**", lv, "*.csv")
        target_files = glob.glob(search_pattern, recursive=True)

        # 补充逻辑：如果有些文件直接在场景文件夹下且文件名包含场景信息
        # 比如统计脚本生成的 CI_Result_oneday_202110.csv
        all_scene_files = glob.glob(os.path.join(STATS_BASE_DIR, "CI_Result_*.csv"))
        
        # 合并所有待处理文件
        process_queue = target_files + all_scene_files
        processed_count = 0

        for file_path in process_queue:
            file_name = os.path.basename(file_path)
            
            # 跳过已生成的文件和映射表
            if "Final" in file_path or "Mapping" in file_path or "Merged" in file_name:
                continue

            try:
                # 读取统计数据
                df_ci = pd.read_csv(file_path)
                
                # 检查该文件是否包含当前 Level 的数据
                # 如果文件里没有 Level 列，或者 Level 列不匹配当前循环，则跳过
                if "Level" in df_ci.columns:
                    if not (df_ci["Level"] == lv).any():
                        continue
                    df_ci_target = df_ci[df_ci["Level"] == lv].copy()
                else:
                    # 如果没有 Level 列（保底情况），尝试从路径判断
                    if lv not in file_path:
                        continue
                    df_ci_target = df_ci.copy()

                # --- 执行挂接 ---
                # 基于 Region 挂接（因为上面已经过滤了 Level）
                df_merged = pd.merge(
                    df_ci_target,
                    df_map_lv,
                    on=["Level", "Region"],
                    how="left"
                )

                # --- 列排序整理 ---
                # 核心字段在前
                header_cols = ["Level", "Region", "Full_Path_Name", "gaul0_name"]
                if lv == "L2":
                    header_cols += ["gaul1_name", "gaul2_name"]
                
                header_cols += ["Scene", "Year"]
                
                # 排除不存在的列并合并剩余列
                final_header = [c for c in header_cols if c in df_merged.columns]
                other_cols = [c for c in df_merged.columns if c not in final_header]
                df_merged = df_merged[final_header + other_cols]

                # --- 保存到对应的 L0 或 L2 文件夹 ---
                out_name = f"Mapped_{file_name}"
                save_path = os.path.join(lv_output_dir, out_name)
                df_merged.to_csv(save_path, index=False, encoding='utf-8-sig')
                processed_count += 1

            except Exception as e:
                print(f"⚠️ 处理文件 {file_name} 时出错: {e}")

        print(f"✅ {lv} 处理完成，生成了 {processed_count} 个挂接文件。")

    print(f"\n✨ 全部任务完成！")
    print(f"📁 结果保存在: {FINAL_OUTPUT_ROOT}")

if __name__ == "__main__":
    merge_ci_by_level()