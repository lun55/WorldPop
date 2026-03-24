import os, glob, pandas as pd, numpy as np
import math  # 导入标准数学模块

# ========== 用户配置区 ==========
input_dir  = r"F:\机场噪音\CI_input\long_table"
output_dir = r"F:\机场噪音\CI_results_advanced"
os.makedirs(output_dir, exist_ok=True)

# Bootstrap 参数：建议初步测试设为 500，正式出图设为 2000
N_BOOT = 1 
SEED   = 42
# ==============================

def normal_cdf(x):
    """标准正态分布累积分布函数 - 修正版本"""
    # 使用 math.erf 代替 np.math.erf
    return (1 + math.erf(x / math.sqrt(2))) / 2

def calc_ci_once(df, pop_col, gdp_col, db_col):
    """基于 Wagstaff 协方差逻辑计算一次 CI"""
    df = df.copy()
    # 1. 按 GDP 升序排列 (穷 -> 富)
    df = df.sort_values(gdp_col)
    
    # 2. 计算健康负担 (HB)
    df['HB'] = df[pop_col] * df[db_col]
    
    pop_sum = df[pop_col].sum()
    hb_sum = df['HB'].sum()
    
    if pop_sum == 0 or hb_sum == 0:
        return np.nan

    # 3. 计算人口占比 (w) 和 负担占比 (h)
    df['w'] = df[pop_col] / pop_sum
    df['h'] = df['HB'] / hb_sum
    # 4. 计算累积人口比例 R
    df['R'] = df['w'].cumsum()
    
    # 5. Wagstaff CI 公式: 2 * sum(h_i * R_i) - 1
    ci = 2 * np.sum(df['h'] * df['R']) - 1
    return ci

def calculate_ci_advanced(group):
    """
    高级 CI 计算：集成 Bootstrap 抽样以获取 SE、P值和置信区间
    """
    # 数据清洗
    valid_group = group[(group['pop_exposed'] > 0) & (group['per_capita_gdp'] > 0)].copy()
    
    if len(valid_group) < 2:
        return pd.Series({
            'CI': np.nan, 'SE': np.nan, 'p_value': np.nan, 
            'CI_95_low': np.nan, 'CI_95_high': np.nan, 'total_pop': group['pop_exposed'].sum()
        })

    # A. 计算原始观测到的 CI
    ci_obs = calc_ci_once(valid_group, 'pop_exposed', 'per_capita_gdp', 'y_mid')

    # B. Bootstrap 抽样检验
    np.random.seed(SEED)
    boot_cis = []
    n = len(valid_group)
    
    for _ in range(N_BOOT):
        # 行随机抽样（带放回）
        sample_idx = np.random.choice(n, size=n, replace=True)
        boot_df = valid_group.iloc[sample_idx]
        ci_b = calc_ci_once(boot_df, 'pop_exposed', 'per_capita_gdp', 'y_mid')
        if not np.isnan(ci_b):
            boot_cis.append(ci_b)

    boot_cis = np.array(boot_cis)
    
    # C. 统计量计算
    if len(boot_cis) < 10: 
        return pd.Series({'CI': ci_obs, 'SE': np.nan, 'p_value': np.nan, 'CI_95_low': np.nan, 'CI_95_high': np.nan, 'total_pop': valid_group['pop_exposed'].sum()})
        
    se = np.std(boot_cis, ddof=1)
    ci_low = np.percentile(boot_cis, 2.5)
    ci_high = np.percentile(boot_cis, 97.5)
    
    # 计算 P 值
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
    print(f"🚀 正在运行 Bootstrap (N={N_BOOT}): {file_name} ...")
    
    df = pd.read_csv(file)
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    try:
        # 统一使用新的 pandas 接口
        results = df.groupby(gaul_cols, group_keys=False).apply(calculate_ci_advanced, include_groups=False).reset_index()
        
        if 'CI' in results.columns:
            final_results = results.dropna(subset=['CI']).copy()
            final_results['is_significant'] = final_results['p_value'] < 0.05
            
            out_name = file_name.replace("_gdp_exposure.csv", "_CI_boot_results.csv")
            save_path = os.path.join(output_dir, out_name)
            
            final_results.to_csv(save_path, index=False)
            print(f"✅ 完成: {out_name}")
            
    except Exception as e:
        print(f"❌ 处理 {file_name} 时出错: {e}")

print(f"\n🎉 任务结束！")