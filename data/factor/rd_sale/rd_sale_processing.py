"""
rd_sale因子处理[研发投入相对于销售额来度量公司研发强度]

有:
XRD / SALE
即 B001216000 / B001100000

其中:
利润表[ B001216000(研发费用) / B001100000(营业总收入) ],

"""
import os
import glob
import pandas as pd

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
    base_dir = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor_data_storage"
    out_dir = "/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/rd_sale"
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "rd_sale_1.csv")

    # 1. 读入利润表数据
    inc = read_and_concat(os.path.join(base_dir, "利润表/第一组(00)"))

    # 2. 标准化列名和日期格式
    print("正在标准化列名和日期格式...")
    inc.rename(columns={'报告期': 'Accper'}, inplace=True)
    inc['Accper'] = pd.to_datetime(inc['Accper'], errors='coerce', format="%Y-%m-%d")
    print("完成\n")

    # 3. 提取并转换字段类型，计算 rd_sale
    print("提取并重命名需用字段，并转换数值类型...")
    inc['B001216000'] = pd.to_numeric(inc['B001216000'], errors='coerce')
    inc['B001100000'] = pd.to_numeric(inc['B001100000'], errors='coerce')
    inc['rd_sale'] = inc['B001216000'] / inc['B001100000']
    print("完成\n")

    # 4. 过滤异常值
    print("过滤异常值...")
    before = len(inc)
    df = inc[(inc['B001100000'] > 0) & inc['B001216000'].notnull()]
    after = len(df)
    print(f"过滤后行数 {before} ➔ {after}\n")

    # 5. 导出结果
    print(f"正在导出到：{out_file}")
    df[['Stkcd','Accper','rd_sale']].to_csv(out_file, index=False, encoding='utf_8_sig')
    print(f"导出完成，共 {len(df)} 条记录")


if __name__ == "__main__":
    main()
