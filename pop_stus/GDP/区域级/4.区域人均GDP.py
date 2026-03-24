import pandas as pd
import os

# -------------------------------
# 文件路径设置
# -------------------------------
YEAR = 2020
# 人口数据文件模板
pop_files = {
    'L0': r"F:\机场噪音\区域总人口\L0_total_pop_{}.csv",  # {} 为年份
    'L1': r"F:\机场噪音\区域总人口\L1_total_pop_{}.csv",
    'L2': r"F:\机场噪音\区域总人口\L2_total_pop_{}.csv"
}

# GDP 数据文件（2020固定）
gdp_files = {
    'L0': rf"F:\机场噪音\GDP\L0_GDP_{YEAR}.csv",
    'L1': rf"F:\机场噪音\GDP\L1_GDP_{YEAR}.csv",
    'L2': rf"F:\机场噪音\GDP\L2_GDP_{YEAR}.csv"
}

# 输出文件夹
output_folder = rf"F:\机场噪音\人均GDP\{YEAR}"
os.makedirs(output_folder, exist_ok=True)

# -------------------------------
# 年份列表
# -------------------------------
years = [2021, 2022, 2023]

# -------------------------------
# 批量处理每个级别
# -------------------------------
for level in ['L0', 'L1', 'L2']:
    print(f"正在处理 {level} ...")
    
    # 读取 GDP（2020）
    gdp_df = pd.read_csv(gdp_files[level])
    
    # 确定合并列 gaul0_name/gaul1_name/gaul2_name
    merge_cols = [col for col in gdp_df.columns if col.startswith('gaul')]
    
    # 遍历每一年的人口数据
    for year in years:
        pop_file = pop_files[level].format(year)
        pop_df = pd.read_csv(pop_file)
        
        # 合并人口数据
        df = pd.merge(gdp_df, pop_df, on=merge_cols, how='left')
        
        # 计算当年人均GDP
        df['per_capita_gdp'] = df['total_gdp'] / df['total_pop']
        
        # 保存单独文件
        out_file = os.path.join(output_folder, f"{level}_pop{year}_per_capita_gdp2020.csv")
        df.to_csv(out_file, index=False)
        
        print(f"{level} {year} 完成，输出文件：{out_file}")