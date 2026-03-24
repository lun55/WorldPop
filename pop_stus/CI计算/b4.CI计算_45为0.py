import os
import glob
import pandas as pd
import numpy as np
import math

# ========== 用户配置区 ==========
# 输入目录：包含 per_capita_gdp, pop_exposed, y_mid 列的长表 CSV
input_dir  = r"F:\备份\机场噪音\CI_input\long_table\oneday"
# 输出目录
output_dir = r"F:\备份\机场噪音\CI_results_P\小于45影响为0"
os.makedirs(output_dir, exist_ok=True)

# Bootstrap 参数
N_BOOT = 200    # 正式论文建议设为 1000 或 2000
SEED   = 42     # 固定随机种子确保结果可重复
THRESHOLD = 45  # 噪音起算阈值（分贝）
# ==============================

def normal_cdf(x):
    """标准正态分布累积分布函数 (用于计算 P 值)"""
    return (1 + math.erf(x / math.sqrt(2))) / 2

def calc_ci_with_threshold(df, threshold=45):
    """
    带阈值处理的核心 CI 计算函数 [cite: 7, 16]：
    1. 将低于 threshold (45dB) 的 y_mid 视为 0 负担。
    2. 使用中点秩修正的协方差法，消除离散分组偏差 [cite: 16, 19]。
    """
    # 1. 排序：按人均 GDP 从低到高 [cite: 11, 15]
    df_sorted = df.sort_values('per_capita_gdp').copy()
    
    pop_exposed = df_sorted['pop_exposed'].values
    y_mid = df_sorted['y_mid'].values
    
    # 2. 阈值预处理：原本小于 45 的视为 0 [意图确认]
    y_effective = np.where(y_mid < threshold, 0, y_mid)
    
    # 3. 计算基础权重
    pop_sum = pop_exposed.sum()
    if pop_sum == 0: 
        return np.nan
    
    # 4. 计算噪音负担占比 (h_i) 和人口占比 (w_i) [cite: 19, 20]
    # noise_load = 暴露人数 * 有效暴露强度
    noise_load = pop_exposed * y_effective
    noise_sum = noise_load.sum()
    
    if noise_sum == 0:
        return 0.0  # 若该区域所有点位均低于 45dB，则认为无不平等分布
    
    h = noise_load / noise_sum  # 噪音负担占比 [cite: 20]
    w = pop_exposed / pop_sum   # 人口占比 [cite: 18]
    
    # 5. 计算中点累积排位 R_i (Mid-point Rank) [cite: 17, 19]
    # 公式: R_i = sum(w_{j<i}) + 0.5 * w_i
    cum_w = np.cumsum(w)
    R_mid = cum_w - 0.5 * w
    
    # 6. 计算集中指数 CI [cite: 8, 19]
    # 公式: CI = 2 * sum(h_i * R_i) - 1
    ci = 2 * np.sum(h * R_mid) - 1
    
    return ci

def calculate_ci_with_bootstrap(group):
    """
    对每个地理单元执行原始计算 + Bootstrap 统计检验 [cite: 21-28]
    """
    # 过滤无效数据：确保人口和 GDP 均大于 0
    valid_group = group[(group['pop_exposed'] > 0) & (group['per_capita_gdp'] > 0)].copy()
    
    if len(valid_group) < 2:
        return pd.Series({
            'CI': np.nan, 'SE': np.nan, 'p_value': np.nan, 
            'CI_95_low': np.nan, 'CI_95_high': np.nan, 
            'total_pop': group['pop_exposed'].sum()
        })

    # A. 计算原始观测 CI 值 (带 45dB 阈值)
    ci_obs = calc_ci_with_threshold(valid_group, threshold=THRESHOLD)

    # B. Bootstrap 重采样过程 [cite: 22]
    np.random.seed(SEED)
    boot_cis = []
    n = len(valid_group)
    
    for _ in range(N_BOOT):
        # 带放回随机抽样
        sample_idx = np.random.choice(n, size=n, replace=True)
        boot_df = valid_group.iloc[sample_idx]
        
        # 重新计算 CI
        ci_b = calc_ci_with_threshold(boot_df, threshold=THRESHOLD)
        if not np.isnan(ci_b):
            boot_cis.append(ci_b)

    boot_cis = np.array(boot_cis)
    
    # C. 统计指标计算 [cite: 23-28]
    if len(boot_cis) < 10:
        return pd.Series({'CI': ci_obs, 'total_pop': valid_group['pop_exposed'].sum()})
        
    # 计算标准误 SE [cite: 24, 27]
    se = np.std(boot_cis, ddof=1)
    
    # 百分位数法计算 95% 置信区间 [cite: 22]
    ci_low = np.percentile(boot_cis, 2.5)
    ci_high = np.percentile(boot_cis, 97.5)
    
    # 计算双侧 P 值 [cite: 26, 28]
    if se > 0:
        z = ci_obs / se
        p_value = 2 * (1 - normal_cdf(abs(z)))
    else:
        p_value = np.nan

    return pd.Series({
        'CI': ci_obs,
        'SE': se,
        'p_value': p_value,
        'CI_95_low': ci_low,
        'CI_95_high': ci_high,
        'total_pop': valid_group['pop_exposed'].sum()
    })

# ========== 主程序循环 ==========

csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

for file in csv_files:
    file_name = os.path.basename(file)
    print(f"正在处理: {file_name} (阈值={THRESHOLD}dB, Bootstrap N={N_BOOT})")
    
    df = pd.read_csv(file)
    # 动态识别行政区划列 (gaul0, gaul1等)
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    try:
        # 分组计算每座城市/行政区的 CI
        results = df.groupby(gaul_cols, group_keys=False).apply(
            lambda x: calculate_ci_with_bootstrap(x)
        ).reset_index()
        
        if 'CI' in results.columns:
            # 标记统计显著性 [cite: 28]
            final_results = results.dropna(subset=['CI']).copy()
            final_results['is_significant'] = final_results['p_value'] < 0.05
            
            # 保存结果
            out_name = file_name.replace("_gdp_exposure.csv", "_CI_Threshold45_results.csv")
            save_path = os.path.join(output_dir, out_name)
            final_results.to_csv(save_path, index=False)
            print(f"✅ 处理成功: {out_name}")
            
    except Exception as e:
        print(f"❌ 处理 {file_name} 时发生错误: {e}")

print(f"\n 任务全部完成！结果保存在: {output_dir}")