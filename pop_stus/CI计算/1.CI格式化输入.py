import os, re, pandas as pd, numpy as np

# ========== 用户配置区 ==========
# 1. 暴露人口汇总结果路径
root_noise = r"F:\机场噪音\HighDB_Filtered_5_intervals_噪音汇总"   
# 2. 分区间平均 GDP 统计路径
root_ave_gdp = r"F:\机场噪音\HighDB_Filtered_GDP汇总" 
# 3. 输出长表的路径（用于后续计算 CI）
out_dir = r"F:\机场噪音\CI_input\HighDB_Filtered_long_table"   
os.makedirs(out_dir, exist_ok=True)
# ==============================

def read_exposure(csv_path):
    """读取暴露人口 CSV 并转为长表"""
    df = pd.read_csv(csv_path)
    # 识别分贝列（数字列）
    db_cols = [c for c in df.columns if str(c).isdigit()]
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    
    # 逻辑：必须包含 total_pop
    id_vars = gaul_cols + ['total_pop'] if 'total_pop' in df.columns else gaul_cols
    
    # 统一标记周期
    period = 'night' if 'night' in os.path.basename(csv_path).lower() else 'oneday'
    
    # 宽转长
    long = df.melt(id_vars=id_vars,
                   value_vars=db_cols,
                   var_name='high_dB', value_name='pop_exposed')
    
    long['high_dB'] = long['high_dB'].astype(int)
    long['y_mid'] = long['high_dB'] - 2.5
    long['period'] = period
    return long

def read_ave_gdp_long(level, year, period):
    """读取分区间 GDP 汇总 CSV 并转为长表"""
    gdp_dir = os.path.join(root_ave_gdp, year, f"L{level}")
    if not os.path.exists(gdp_dir):
        return None
    
    # 显式查找 GDP 文件
    all_gdp_files = os.listdir(gdp_dir)
    target_files = [f for f in all_gdp_files 
                    if period in f.lower() and f.lower().endswith('_ave_gdp_stats.csv')]
    
    if not target_files:
        return None
    
    df = pd.read_csv(os.path.join(gdp_dir, target_files[0]))
    gaul_cols = [c for c in df.columns if c.startswith('gaul')]
    val_cols = [c for c in df.columns if c.startswith('ave_gdp_')]
    
    # 宽转长
    long = df.melt(id_vars=gaul_cols, value_vars=val_cols, 
                   var_name='dB_str', value_name='per_capita_gdp')
    
    # 从 'ave_gdp_45dB' 提取 45
    long['high_dB'] = long['dB_str'].str.extract(r'(\d+)').astype(int)
    return long.drop(columns=['dB_str'])

# ========== 主程序逻辑 ==========

# 遍历年份文件夹 (202110, 202210, 202310, 总)
years = [d for d in os.listdir(root_noise) if os.path.isdir(os.path.join(root_noise, d))]

for yr_fold in years:
    for level in [0, 1, 2]:
        # 确定当前搜索路径
        current_noise_dir = os.path.join(root_noise, yr_fold, f"L{level}")
        if not os.path.exists(current_noise_dir):
            continue
            
        all_noise_files = os.listdir(current_noise_dir)
        
        # 统一处理 oneday 和 night
        for p in ['oneday', 'night']:
            # 1. 筛选当前层级、当前周期的暴露人口文件
            exp_files = [os.path.join(current_noise_dir, f) for f in all_noise_files 
                         if p in f.lower() and f.lower().endswith('_exposure.csv')]
            
            if not exp_files:
                continue
            
            # 读取所有匹配的文件并合并
            try:
                pieces = [read_exposure(f) for f in exp_files]
                lvl_exp_df = pd.concat(pieces, ignore_index=True)
            except Exception as e:
                print(f"❌ 读取暴露量文件出错 ({yr_fold} L{level} {p}): {e}")
                continue
            
            # 2. 读取对应的分区间平均 GDP 长表
            lvl_gdp_df = read_ave_gdp_long(level, yr_fold, p)
            
            if lvl_gdp_df is None:
                print(f"⚠️ 找不到对应的 GDP 汇总表: {yr_fold} L{level} {p}")
                continue
            
            # 3. 合并：地理标识符 + 分贝等级
            # 自动提取 gaul 开头的共同列
            common_gaul = [c for c in lvl_exp_df.columns if c.startswith('gaul') and c in lvl_gdp_df.columns]
            merge_keys = common_gaul + ['high_dB']
            
            final_df = pd.merge(lvl_exp_df, lvl_gdp_df, on=merge_keys, how='left')
            
            # 填充缺失 GDP 为 0（例如某些极高分贝区无人居住）
            final_df['per_capita_gdp'] = final_df['per_capita_gdp'].fillna(0)
            
            # 4. 导出
            if not final_df.empty:
                out_name = f"{yr_fold}_L{level}_{p}_gdp_exposure.csv"
                save_path = os.path.join(out_dir, out_name)
                final_df.to_csv(save_path, index=False)
                print(f"✅ 成功生成长表: {out_name}")

print("\n🎉 所有任务处理完毕！长表已存入 CI_input\\long_table 文件夹。")