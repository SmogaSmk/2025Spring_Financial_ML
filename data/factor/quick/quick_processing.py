import os
import glob
import pandas as pd

"""
quick 因子处理
提取现成的速动比率因子 (F010201A)

其中：
偿债能力[ F010201A(速动比率) ]
"""

def read_and_concat(dir_path: str) -> pd.DataFrame:
    print(f"开始读取目录：{dir_path}")
    patterns = ['*.xlsx', '*.xls']
    dfs = []
    for pattern in patterns:
        for f in glob.glob(os.path.join(dir_path, pattern)):
            name = os.path.basename(f)
            if name.startswith('~$'):
                print(f"跳过临时文件：{name}")
                continue
            try:
                df = pd.read_excel(f, dtype={'Stkcd': str})
                dfs.append(df)
                print(f"已读取：{name}（共 {len(df)} 行）")
            except Exception as e:
                print(f"读取失败：{name}，原因：{e}")
    if not dfs:
        print(f"警告：目录 {dir_path} 下未找到任何 Excel 文件，返回空 DataFrame")
        return pd.DataFrame()
    concatenated = pd.concat(dfs, ignore_index=True)
    print(f"合并完成，共 {len(concatenated)} 行\n")
    return concatenated


def main():
    base_dir = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor_data_storage"
    in_dir = os.path.join(base_dir, "偿债能力/第一组(00)")
    out_dir = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/quick"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "quick_1.csv")

    # 1. 读取数据
    df = read_and_concat(in_dir)
    if df.empty:
        print("错误：未读取到任何数据，退出处理。")
        return

    # 2. 标准化列名与日期
    print("正在标准化列名和日期格式...")
    df.rename(columns={'报告期': 'Accper'}, inplace=True)
    df['Accper'] = pd.to_datetime(df['Accper'], errors='coerce', format="%Y-%m-%d")
    print("完成\n")

    # 3. 提取速动比率并转换类型
    print("提取速动比率字段并转换类型...")
    df = df[['Stkcd', 'Accper', 'F010201A']].rename(columns={'F010201A': 'quick'})
    df['quick'] = pd.to_numeric(df['quick'], errors='coerce')
    print("完成\n")

    # 4. 过滤无效值
    before = len(df)
    df = df[df['quick'].notnull()]
    after = len(df)
    print(f"过滤后行数 {before} ➔ {after}\n")

    # 5. 导出结果
    print(f"正在导出到：{out_file}")
    df[['Stkcd', 'Accper', 'quick']].to_csv(out_file, index=False, encoding='utf_8_sig')
    print(f"导出完成，共 {len(df)} 条记录")


if __name__ == "__main__":
    main()
