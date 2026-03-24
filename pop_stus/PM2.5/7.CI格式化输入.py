import os, re, pandas as pd, numpy as np

# ========== 用户配置区 ==========
root_pm25 = r"F:\机场噪音\PM2.5\PM2.5_影响汇总"   
root_ave_gdp = r"F:\机场噪音\PM2.5\PM2.5_GDP汇总" 
out_dir = r"F:\机场噪音\PM2.5\CI_input\long_table"   
os.makedirs(out_dir, exist_ok=True)
# ==============================

def extract_mid_value(range_str):
    """从 '0_5' 提取中值 2.5, 从 '35_50' 提取 42.5"""
    nums = re.findall(r'\d+', str(range_str))
    if len(nums) == 2:
        return (int(nums[0]) + int(nums[1])) / 2
    elif len(nums) == 1:
        return float(nums[0])
    return 0.0

def read_exposure_pm(csv_path):
    """读取 PM2.5 暴露人口 CSV 并转为长表"""
    df = pd.read_csv(csv_path)
    
    # 识别浓度区间列 (特征：包含下划线且不是 gaul 列，或者根据之前的后缀判断)
    # 修正：识别形如 "0_5", "35_50" 的列，排除 _ratio 结尾的列
    pm_range_cols = [c for c in df.columns if re.match(r'^\d+_\d+$', str(c))]
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    id_vars = gaul_cols + ['total_pop'] if 'total_pop' in df.columns else gaul_cols
    
    # 宽转长
    long = df.melt(id_vars=id_vars,
                    value_vars=pm_range_cols,
                    var_name='PM_range', value_name='pop_exposed')
    
    # 计算区间中值，用于后续 CI 计算
    long['pm_mid'] = long['PM_range'].apply(extract_mid_value)
    return long

def read_ave_gdp_long_pm(level, year_folder):
    """读取 PM2.5 分区间 GDP 汇总并转为长表"""
    gdp_dir = os.path.join(root_ave_gdp, year_folder, f"L{level}")
    if not os.path.exists(gdp_dir):
        return None
    
    target_files = [f for f in os.listdir(gdp_dir) if f.endswith('_ave_gdp_stats.csv')]
    if not target_files:
        return None
    
    df = pd.read_csv(os.path.join(gdp_dir, target_files[0]))
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    val_cols = [c for c in df.columns if c.startswith('ave_gdp_')]
    
    # 宽转长
    long = df.melt(id_vars=gaul_cols, value_vars=val_cols, 
                    var_name='range_str', value_name='per_capita_gdp')
    
    # 从 'ave_gdp_0_5' 提取 '0_5'
    long['PM_range'] = long['range_str'].str.replace('ave_gdp_', '')
    return long.drop(columns=['range_str'])

# ========== 主程序逻辑 ==========

# 遍历年份文件夹 (如 202210)
years = [d for d in os.listdir(root_pm25) if os.path.isdir(os.path.join(root_pm25, d))]

for yr_fold in years:
    # 之前脚本里有 oneday/night，PM2.5 数据通常不分昼夜，这里简化处理
    for level in [0, 1, 2]:
        current_exp_dir = os.path.join(root_pm25, yr_fold, f"L{level}")
        if not os.path.exists(current_exp_dir):
            continue
            
        # 搜索暴露量文件
        exp_files = [os.path.join(current_exp_dir, f) for f in os.listdir(current_exp_dir) 
                     if f.endswith('_exposure.csv')]
        
        if not exp_files:
            continue

        # 1. 读取并合并暴露量
        try:
            pieces = [read_exposure_pm(f) for f in exp_files]
            lvl_exp_df = pd.concat(pieces, ignore_index=True)
        except Exception as e:
            print(f"❌ 读取暴露量文件出错 ({yr_fold} L{level}): {e}")
            continue
        
        # 2. 读取 GDP
        lvl_gdp_df = read_ave_gdp_long_pm(level, yr_fold)
        
        if lvl_gdp_df is None:
            print(f"⚠️ 找不到 GDP 汇总表: {yr_fold} L{level}")
            continue
        
        # 3. 合并
        common_gaul = [c for c in lvl_exp_df.columns if c.startswith('gaul') and c in lvl_gdp_df.columns]
        merge_keys = common_gaul + ['PM_range']
        
        final_df = pd.merge(lvl_exp_df, lvl_gdp_df, on=merge_keys, how='left')
        final_df['per_capita_gdp'] = final_df['per_capita_gdp'].fillna(0)
        
        # 4. 导出
        if not final_df.empty:
            out_name = f"{yr_fold}_L{level}_pm25_gdp_exposure.csv"
            save_path = os.path.join(out_dir, out_name)
            final_df.to_csv(save_path, index=False)
            print(f"✅ 成功生成 PM2.5 长表: {out_name}")

print("\n🎉 处理完毕！长表已存入 CI_input\\long_table。")