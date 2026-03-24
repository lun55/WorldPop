import os, glob, pandas as pd, numpy as np
import math

# ========== 用户配置区 ==========
input_dir  = r"F:\备份\机场噪音\CI_input\long_table"
output_dir = r"F:\备份\机场噪音\CI_results_P\小于45影响为0"
os.makedirs(output_dir, exist_ok=True)

# Bootstrap 参数：建议初步测试设为 500，正式科研出图设为 2000
N_BOOT = 200
SEED   = 42
# ==============================

def normal_cdf(x):
    """标准正态分布累积分布函数"""
    return (1 + math.erf(x / math.sqrt(2))) / 2
    
def calc_ci_with_threshold(df, threshold=45):
    """
    带阈值处理的核心 CI 计算函数：
    1. 排除低于阈值（如45dB）的噪音负担。
    2. 使用中点秩修正的协方差法，消除离散分组偏差。
    
    参数:
    df: 包含 'per_capita_gdp', 'pop_exposed', 'y_mid' 的 DataFrame
    threshold: 噪音起算阈值，默认为 45
    """
    # 1. 排序：按人均GDP从低到高 [cite: 11, 15]
    df_sorted = df.sort_values('per_capita_gdp').copy()
    
    pop_exposed = df_sorted['pop_exposed'].values
    y_mid = df_sorted['y_mid'].values
    
    # 2. 阈值预处理：低于 45dB 的视为 0 负担
    # 这样 y_mid 仅保留具有公共健康意义的暴露强度
    y_effective = np.where(y_mid < threshold, 0, y_mid)
    
    # 3. 计算权重与比例
    pop_sum = pop_exposed.sum()
    if pop_sum == 0: 
        return np.nan
    
    # 计算各组噪音负担 (h_i) [cite: 19-20]
    # h_i = (w_i * y_i) / mu 
    noise_load = pop_exposed * y_effective
    noise_sum = noise_load.sum()
    
    if noise_sum == 0:
        return 0.0  # 若全低于阈值，则认为无不平等分布
    
    h = noise_load / noise_sum  # 噪音负担占比
    w = pop_exposed / pop_sum   # 人口占比
    
    # 4. 计算中点累积排位 R_i (Mid-point Rank) [cite: 17, 19]
    # 公式: R_i = sum(w_{j<i}) + 0.5 * w_i
    cum_w = np.cumsum(w)
    R_mid = cum_w - 0.5 * w
    
    # 5. 计算集中指数 CI [cite: 8, 19]
    # 公式: CI = 2 * sum(h_i * R_i) - 1
    ci = 2 * np.sum(h * R_mid) - 1
    
    return ci

def calc_ci_core(df):
    """
    核心 CI 计算函数：
    使用中点秩修正的协方差法，其结果等价于梯形面积法，
    且消除了离散分组导致的数值偏高问题。
    """
    # 1. 排序：从穷到富
    df = df.sort_values('per_capita_gdp')
    
    # 2. 计算权重
    pop_exposed = df['pop_exposed'].values
    y_mid = df['y_mid'].values
    
    pop_sum = pop_exposed.sum()
    if pop_sum == 0: return np.nan
    
    # 3. 计算负担占比 (h) 和人口占比 (w)
    noise_load = pop_exposed * y_mid
    noise_sum = noise_load.sum()
    if noise_sum == 0: return 0.0
    
    h = noise_load / noise_sum
    w = pop_exposed / pop_sum
    
    # 4. 关键修正：计算中点累积排位 (Mid-point Rank)
    # 这步确保了离散分贝区间计算结果与面积法一致
    cum_w = np.cumsum(w)
    R_mid = cum_w - 0.5 * w
    
    # 5. 计算 CI
    ci = 2 * np.sum(h * R_mid) - 1
    return ci

def calculate_ci_with_bootstrap(group):
    """
    对每个分组执行原始计算 + Bootstrap 统计检验
    """
    # 过滤无效数据
    valid_group = group[(group['pop_exposed'] > 0) & (group['per_capita_gdp'] > 0)].copy()
    
    # 至少需要两个数据点才能计算分布
    if len(valid_group) < 2:
        return pd.Series({
            'CI': np.nan, 'SE': np.nan, 'p_value': np.nan, 
            'CI_95_low': np.nan, 'CI_95_high': np.nan, 
            'total_pop': group['pop_exposed'].sum()
        })

    # A. 计算原始观测 CI 值
    ci_obs = calc_ci_core(valid_group)

    # B. Bootstrap 过程
    np.random.seed(SEED)
    boot_cis = []
    n = len(valid_group)
    
    for _ in range(N_BOOT):
        # 带放回重采样
        sample_idx = np.random.choice(n, size=n, replace=True)
        boot_df = valid_group.iloc[sample_idx]
        
        # 重新抽样后的有效性检查
        if boot_df['pop_exposed'].sum() > 0:
            ci_b = calc_ci_core(boot_df)
            if not np.isnan(ci_b):
                boot_cis.append(ci_b)

    boot_cis = np.array(boot_cis)
    
    # C. 统计指标计算
    if len(boot_cis) < 10:  # 采样成功数太少
        return pd.Series({'CI': ci_obs, 'SE': np.nan, 'p_value': np.nan, 'CI_95_low': np.nan, 'CI_95_high': np.nan, 'total_pop': valid_group['pop_exposed'].sum()})
        
    se = np.std(boot_cis, ddof=1)
    ci_low = np.percentile(boot_cis, 2.5)   # 95%置信区间下限
    ci_high = np.percentile(boot_cis, 97.5) # 95%置信区间上限
    
    # 计算双侧 P 值 (基于正态分布假设)
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
    print(f"🚀 正在处理 (Bootstrap N={N_BOOT}): {file_name} ...")
    
    df = pd.read_csv(file)
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    try:
        # 修正后的写法：删掉 include_groups，保持兼容性
        results = df.groupby(gaul_cols, group_keys=False).apply(
            lambda x: calculate_ci_with_bootstrap(x)
        ).reset_index()
        
        # 检查返回结果是否是 MultiIndex（旧版 Pandas 特色）
        # 如果是，reset_index() 会自动处理好
        
        if 'CI' in results.columns:
            final_results = results.dropna(subset=['CI']).copy()
            final_results['is_significant'] = final_results['p_value'] < 0.05
            
            out_name = file_name.replace("_gdp_exposure.csv", "_CI_Stats_results.csv")
            save_path = os.path.join(output_dir, out_name)
            final_results.to_csv(save_path, index=False)
            print(f"✅ 成功完成，保存至: {out_name}")
            
    except Exception as e:
        print(f"❌ 处理 {file_name} 时发生错误: {e}")

print(f"\n🎉 所有统计计算已结束！结果目录: {output_dir}")