import os
import pandas as pd

# --- 配置路径 ---
SRC_ROOT = r"F:\机场噪音\Noise_Population_Stats_New"
DST_ROOT = r"F:\机场噪音\Noise_Population_Stats_矫正"

# 基准文件夹（2022和2023的计算都以 202110 为原始数据源）
BASE_FOLDER = "202110"

# --- 系数与年份配置 ---
# 2023 的系数 = 2022系数 * 2023增量系数
coef_2022 = 1.458548397
coef_2023 = 1.458548397 * 1.124061502
coef_total = 1.597952785

# 任务列表：目标文件夹 -> (复合系数, 该文件应显示的year_used)
TASKS = {
    "202210": (coef_2022, 2022),
    "202310": (coef_2023, 2023),
    "总":      (coef_total, None)  # "总" 不需要修改 year_used
}

def process_correction():
    for folder, (final_coef, target_year) in TASKS.items():
        print(f"\n>>> 正在处理任务: {folder}")
        print(f"    使用系数: {final_coef:.6f} | 目标年份字段: {target_year}")

        # 确定源数据路径：若是“总”则读“总”，否则读基准“202110”
        current_src_top = os.path.join(SRC_ROOT, "总" if folder == "总" else BASE_FOLDER)
        dst_top = os.path.join(DST_ROOT, folder)

        if not os.path.exists(current_src_top):
            print(f"    ⚠️ 跳过：找不到源文件夹 {current_src_top}")
            continue

        for root, _, files in os.walk(current_src_top):
            for file in files:
                if not file.lower().endswith(".csv"):
                    continue

                src_csv = os.path.join(root, file)
                
                # 1. 构造目标路径与文件名替换 (202110 -> 202210/202310)
                rel_dir = os.path.relpath(root, current_src_top)
                if folder != "总":
                    rel_dir_fixed = rel_dir.replace(BASE_FOLDER, folder)
                    file_fixed = file.replace(BASE_FOLDER, folder)
                else:
                    rel_dir_fixed = rel_dir
                    file_fixed = file

                dst_dir = os.path.join(dst_top, rel_dir_fixed)
                os.makedirs(dst_dir, exist_ok=True)
                dst_csv = os.path.join(dst_dir, file_fixed)

                # 2. 读取与矫正数据
                try:
                    df = pd.read_csv(src_csv)
                    
                    # 仅针对中国数据进行矫正
                    if 'gaul0_name' in df.columns and 'affected_pop' in df.columns:
                        mask = df.gaul0_name == "China"
                        df.loc[mask, "affected_pop"] *= final_coef
                        
                        # 修改 year_used 列（如果不是“总”且列存在）
                        if target_year is not None and 'year_used' in df.columns:
                            df['year_used'] = target_year
                    
                    # 3. 保存结果
                    df.to_csv(dst_csv, index=False, encoding="utf-8")
                    
                except Exception as e:
                    print(f"    ❌ 文件处理出错 {file}: {e}")

        print(f"    ✅ {folder} 处理完成！")

if __name__ == "__main__":
    process_correction()
    print("\n🎉 全部数据矫正及年份修改完毕！")