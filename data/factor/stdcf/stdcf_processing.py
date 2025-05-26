"""
stdcf因子处理(16个季度的“净现金流除以销售额”的标准差。)

有:
C001000000 / B001100000 的标准差

其中:
企业现金流量表[ 经营活动产生的现金流量净额(C001000000) ]
利润表[ 营业总收入(B001100000) ]
"""
import os
import glob
import pandas as pd

def read_and_concat(dir_path: str) -> pd.DataFrame:
    print(f"开始读取目录：{dir_path}")
    # 支持 .xlsx 和 .xls 文件
    patterns = ["*.xlsx", "*.xls"]
    dfs = []
    for pattern in patterns:
        for f in glob.glob(os.path.join(dir_path, pattern)):
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
    if not dfs:
        print(f"警告: 目录 {dir_path} 下没有找到符合模式的 Excel 文件，返回空 DataFrame")
        return pd.DataFrame()
    concatenated = pd.concat(dfs, ignore_index=True)
    print(f"合并完成，共 {len(concatenated)} 行\n")
    return concatenated


def main():
    base = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor_data_storage"
    out_dir = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/stdcf"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "stdcf_1.csv")

    # 1. 读取现金流量表和利润表数据
    cf = read_and_concat(os.path.join(base, "现金流量表/第一组(00)"))
    pf = read_and_concat(os.path.join(base, "利润表/第一组(00)"))

    # 处理读取失败返回空 DataFrame 的情况
    if cf.empty or pf.empty:
        print("错误: Cash Flow 或 Profit 数据为空，检查输入目录和文件格式。")
        return

    # 2. 标准化列名及日期格式
    print("正在标准化列名和日期格式...")
    for df in (cf, pf):
        df.rename(columns={'报告期': 'Accper'}, inplace=True)
        df['Accper'] = pd.to_datetime(df['Accper'], errors='coerce', format="%Y-%m-%d")
    print("完成\n")

    # 3. 提取所需字段并转换类型
    print("提取并转换字段类型...")
    cf = cf[['Stkcd', 'Accper', 'C001000000']].rename(columns={'C001000000': 'CFO'})
    cf['CFO'] = pd.to_numeric(cf['CFO'], errors='coerce')

    pf = pf[['Stkcd', 'Accper', 'B001100000']].rename(columns={'B001100000': 'saleq'})
    pf['saleq'] = pd.to_numeric(pf['saleq'], errors='coerce')
    print("完成\n")

    # 4. 合并表格
    print("正在合并表格...")
    df = pd.merge(cf, pf, on=['Stkcd', 'Accper'], how='inner')
    print(f"合并后共 {len(df)} 行\n")

    # 5. 计算季度比率并做滚动标准差
    print("计算 stdcf 因子...")
    df['saleq_adj'] = df['saleq'].replace(0, 0.01)
    df['ratio'] = df['CFO'] / df['saleq_adj']
    df.sort_values(['Stkcd', 'Accper'], inplace=True)
    df['stdcf'] = df.groupby('Stkcd')['ratio']\
                    .transform(lambda x: x.rolling(window=16, min_periods=16).std())

    # 6. 过滤缺失值
    before = len(df)
    df = df[df['stdcf'].notnull()]
    after = len(df)
    print(f"过滤后行数 {before} ➔ {after}\n")

    # 7. 导出结果
    print(f"正在导出到：{out_file}")
    df[['Stkcd', 'Accper', 'stdcf']].to_csv(out_file, index=False, encoding='utf_8_sig')
    print(f"导出完成，共 {len(df)} 条记录")


if __name__ == "__main__":
    main()
