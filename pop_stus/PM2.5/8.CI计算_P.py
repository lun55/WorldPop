import os, glob, pandas as pd, numpy as np
import math

# ========== 用户配置区 ==========
input_dir  = r"F:\机场噪音\PM2.5\CI_input\long_table"
output_dir = r"F:\机场噪音\PM2.5\CI_results_P"
os.makedirs(output_dir, exist_ok=True)

# Bootstrap 参数
N_BOOT = 1000
SEED   = 42
# ==============================

def normal_cdf(x):
    """标准正态分布累积分布函数"""
    return (1 + math.erf(x / math.sqrt(2))) / 2

def calc_ci_core(df):
    """
    核心 CI 计算函数：
    使用中点秩修正的协方差法。
    """
    # 1. 排序：从穷到富 (必须确保排序)
    df = df.sort_values('per_capita_gdp')
    
    # 2. 获取核心数值
    pop_exposed = df['pop_exposed'].values
    # 【修改点】使用长表中的 pm_mid 字段
    pm_mid = df['pm_mid'].values
    
    pop_sum = pop_exposed.sum()
    if pop_sum == 0: return np.nan
    
    # 3. 计算环境污染负担占比 (h)
    # 负担 = 人口 * 浓度
    pm_load = pop_exposed * pm_mid
    pm_sum = pm_load.sum()
    if pm_sum == 0: return 0.0
    
    h = pm_load / pm_sum # 负担份额
    w = pop_exposed / pop_sum # 人口份额
    
    # 4. 计算中点累积排位 (Mid-point Rank)
    cum_w = np.cumsum(w)
    R_mid = cum_w - 0.5 * w
    
    # 5. 计算 CI (范围从 -1 到 1)
    ci = 2 * np.sum(h * R_mid) - 1
    return ci

def calculate_ci_with_bootstrap(group):
    """
    对每个分组执行原始计算 + Bootstrap 统计检验
    """
    # 过滤无效数据：必须有人口暴露且有经济数据
    valid_group = group[(group['pop_exposed'] > 0) & (group['per_capita_gdp'] >= 0)].copy()
    
    # 至少需要两个不同的 PM2.5 区间点才能计算分布
    if len(valid_group.drop_duplicates('pm_mid')) < 2:
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
        # 带放回重采样：在 PM2.5 区间层面重采样
        sample_idx = np.random.choice(n, size=n, replace=True)
        boot_df = valid_group.iloc[sample_idx]
        
        if boot_df['pop_exposed'].sum() > 0:
            ci_b = calc_ci_core(boot_df)
            if not np.isnan(ci_b):
                boot_cis.append(ci_b)

    boot_cis = np.array(boot_cis)
    
    # C. 统计指标计算
    if len(boot_cis) < 50: # 有效采样数太少则认为不可靠
        return pd.Series({'CI': ci_obs, 'SE': np.nan, 'p_value': np.nan, 'CI_95_low': np.nan, 'CI_95_high': np.nan, 'total_pop': valid_group['pop_exposed'].sum()})
        
    se = np.std(boot_cis, ddof=1)
    ci_low = np.percentile(boot_cis, 2.5)
    ci_high = np.percentile(boot_cis, 97.5)
    
    # 计算双侧 P 值
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
    out_name = file_name.replace("_gdp_exposure.csv", "_CI_Results.csv")
    save_path = os.path.join(output_dir, out_name)
    
    if os.path.exists(save_path):
        print(f"⏩ 跳过已存在文件: {out_name}")
        continue
        
    print(f"🚀 正在计算 PM2.5 CI (Bootstrap N={N_BOOT}): {file_name} ...")
    
    df = pd.read_csv(file)
    # 动态识别地理级别列
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    try:
        # 分组计算 (按国家/省/市)
        results = df.groupby(gaul_cols, group_keys=False).apply(
            lambda x: calculate_ci_with_bootstrap(x), 
            include_groups=False
        ).reset_index()
        
        if 'CI' in results.columns:
            final_results = results.dropna(subset=['CI']).copy()
            final_results['is_significant'] = final_results['p_value'] < 0.05
            
            # 导出
            final_results.to_csv(save_path, index=False)
            print(f"✅ 完成: {out_name}")
            
    except Exception as e:
        print(f"❌ 处理 {file_name} 时发生错误: {e}")

print(f"\n🎉 统计计算全部完成！结果保存在: {output_dir}")