import os, glob, pandas as pd, numpy as np

# ========== 用户配置区 ==========
# 输入：之前生成的含有 pop_exposed 和 per_capita_gdp 的长表
input_dir  = r"F:\机场噪音\CI_input\long_table"
# 输出：每个行政区的 CI 计算结果
output_dir = r"F:\机场噪音\CI_results"
os.makedirs(output_dir, exist_ok=True)
# ==============================

def calculate_ci(group):
    """
    核心 CI 计算函数：
    1. 过滤无效数据点
    2. 按分区间平均 GDP 排序
    3. 计算累积占比并使用梯形法求 CI
    """
    # 💡 逻辑修正：过滤掉人口 <= 0 或 GDP <= 0 的点（现实中不可能存在，多为栅格对齐误差）
    valid_group = group[(group['pop_exposed'] > 0) & (group['per_capita_gdp'] > 0)].copy()
    
    # 必须保证至少有 2 个有效的分贝区间数据，才能构成累积分布曲线
    if len(valid_group) < 2:
        return pd.Series({
            'CI': np.nan, 
            'total_exposed_pop': group['pop_exposed'].sum(), 
            'weighted_ave_gdp': np.nan
        })

    # 1. 核心排序：按该区间的人均 GDP 升序排列 (从穷到富)
    valid_group = valid_group.sort_values('per_capita_gdp')

    # 2. 计算累积的人口占比 (x_i)
    pop_sum = valid_group['pop_exposed'].sum()
    valid_group['cum_pop_pct'] = valid_group['pop_exposed'].cumsum() / pop_sum
    
    # 3. 计算累积的噪音暴露贡献占比 (y_i)
    # 噪音负荷 = 暴露人口 * 分贝中值
    valid_group['noise_load'] = valid_group['pop_exposed'] * valid_group['y_mid']
    noise_sum = valid_group['noise_load'].sum()
    
    if noise_sum == 0:
        return pd.Series({'CI': 0, 'total_exposed_pop': pop_sum, 'weighted_ave_gdp': np.nan})
        
    valid_group['cum_noise_pct'] = valid_group['noise_load'].cumsum() / noise_sum

    # 4. 梯形法计算曲线下面积 (AUC)
    x = valid_group['cum_pop_pct'].values
    y = valid_group['cum_noise_pct'].values
    
    # 插入坐标原点 (0,0) 作为曲线起点
    x = np.insert(x, 0, 0)
    y = np.insert(y, 0, 0)
    
    # AUC 计算公式
    auc = np.sum((x[1:] - x[:-1]) * (y[1:] + y[:-1]) / 2)
    # CI = 1 - 2 * AUC
    ci = 1 - 2 * auc
    
    # 计算该区域的加权平均收入
    w_gdp = (valid_group['per_capita_gdp'] * valid_group['pop_exposed']).sum() / pop_sum
    
    return pd.Series({
        'CI': ci,
        'total_exposed_pop': pop_sum,
        'weighted_ave_gdp': w_gdp
    })

# ========== 主程序循环 ==========

csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

if not csv_files:
    print(f"❌ 错误：在 {input_dir} 下未发现 CSV 文件。")

for file in csv_files:
    file_name = os.path.basename(file)
    print(f"🚀 正在计算: {file_name} ...")
    
    df = pd.read_csv(file)
    
    # 自动识别地理层级列 (gaul0, gaul1 等)
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    # 💡 修正 apply 调用：使用 include_groups=False (适配新版 Pandas) 
    # 或者更简单的通过 lambda 显式处理结果
    try:
        results = df.groupby(gaul_cols, group_keys=False).apply(calculate_ci, include_groups=False).reset_index()
    except Exception as e:
        print(f"❌ 分组计算出错: {e}")
        continue
    
    # 安全检查：确保生成了 CI 列
    if 'CI' in results.columns:
        # 移除无法计算的 NaN 结果
        final_results = results.dropna(subset=['CI']).copy()
        
        # 命名转换：202110_L0_oneday_gdp_exposure.csv -> 202110_L0_oneday_CI_results.csv
        out_name = file_name.replace("_gdp_exposure.csv", "_CI_results.csv")
        save_path = os.path.join(output_dir, out_name)
        
        final_results.to_csv(save_path, index=False)
        print(f"✅ 计算完成，保存至: {out_name} (有效区域数: {len(final_results)})")
    else:
        print(f"⚠️ 警告: {file_name} 未能生成有效的 CI 数据列。")

print(f"\n🎉 所有计算任务已完成！结果存放在: {output_dir}")