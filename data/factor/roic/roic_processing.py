import os
import glob
import pandas as pd

"""
投资资本回报率(变体)因子处理

有:
( EBIT - NOPI ) / ( 市值 - 现金 )
即 ( F050601B - ( B001400000 - B001500000 ) ) / ( F100801A -  A001101000 )

其中:
盈利能力[ F050601B(息税前利润（EBIT）],
利润表[ NOPI = B001400000(加：营业外收入) - B001500000(减：营业外支出) ],
相对价值指标[ F100801A(市值A)],
资产负债[ A001101000(货币资金) ]
"""

def read_and_concat(dir_path: str) -> pd.DataFrame:
    print(f"开始读取目录：{dir_path}")
    dfs = []
    for f in glob.glob(os.path.join(dir_path, "*.xlsx")):
        name = os.path.basename(f)
        if name.startswith("~$"):
            print(f"跳过临时文件：{name}")
            continue
        try:
            df = pd.read_excel(f, dtype={'Stkcd': str})
            dfs.append(df)
            print(f"已读取：{name}（共 {len(df)} 行）")
        except Exception as e:
            print(f"读取失败：{name}，原因：{e}")
    concatenated = pd.concat(dfs, ignore_index=True)
    print(f"合并完成，共 {len(concatenated)} 行\n")
    return concatenated

def main():
    base    = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor_data_storage"
    out_dir = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/roic"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "roic_2.csv")

    # 1. 读入各张表
    prof = read_and_concat(os.path.join(base, "盈利能力/第二组(25)"))
    inc  = read_and_concat(os.path.join(base, "利润表/第二组(25)"))
    rv   = read_and_concat(os.path.join(base, "相对价值指标/第二组(25)"))
    bal  = read_and_concat(os.path.join(base, "资产负债表/第二组(25)"))

    # 2. 标准化列名与日期
    print("正在标准化列名和日期格式...")
    for df in (prof, inc, rv, bal):
        df.rename(columns={'报告期': 'Accper'}, inplace=True)
        df['Accper'] = pd.to_datetime(df['Accper'], errors='coerce', format="%Y-%m-%d")
    print("完成\n")

    # 3. 提取并转换字段类型
    print("提取并重命名需用字段，并转换数值类型...")
    prof = prof[['Stkcd','Accper','F050601B']].rename(columns={'F050601B':'EBIT'})
    prof['EBIT'] = pd.to_numeric(prof['EBIT'], errors='coerce')

    # 利润表：先转换，再计算 NOPI
    inc['B001400000'] = pd.to_numeric(inc['B001400000'], errors='coerce')
    inc['B001500000'] = pd.to_numeric(inc['B001500000'], errors='coerce')
    inc['NOPI'] = inc['B001400000'] - inc['B001500000']
    inc  = inc[['Stkcd','Accper','NOPI']]

    rv = rv[['Stkcd','Accper','F100801A']].rename(columns={'F100801A':'MktCap'})
    rv['MktCap'] = pd.to_numeric(rv['MktCap'], errors='coerce')

    bal = bal[['Stkcd','Accper','A001101000']].rename(columns={'A001101000':'Cash'})
    bal['Cash'] = pd.to_numeric(bal['Cash'], errors='coerce')
    print("完成\n")

    # 4. 多表合并
    print("正在合并表格...")
    df = (prof
          .merge(inc, on=['Stkcd','Accper'], how='left')
          .merge(rv,  on=['Stkcd','Accper'], how='left')
          .merge(bal, on=['Stkcd','Accper'], how='left'))
    print(f"合并后共 {len(df)} 行\n")

    # 5. 计算因子并过滤
    print("计算 ROIC_var 并过滤异常值...")
    df['ROIC_var'] = (df['EBIT'] - df['NOPI']) / (df['MktCap'] - df['Cash'])
    before = len(df)
    df = df[df['MktCap'] > df['Cash']]
    df = df[df['EBIT'].notnull()]
    after = len(df)
    print(f"过滤后行数 {before} ➔ {after}\n")

    # 6. 导出结果
    print(f"正在导出到：{out_file}")
    df[['Stkcd','Accper','ROIC_var']].to_csv(out_file, index=False, encoding='utf_8_sig')
    print(f"导出完成，共 {len(df)} 条记录")

if __name__ == "__main__":
    main()
