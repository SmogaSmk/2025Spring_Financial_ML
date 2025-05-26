# coding: utf-8
"""
波动率因子批量处理脚本
"""
import os
import glob
import warnings
import numpy as np
import pandas as pd
from itertools import product

warnings.filterwarnings("ignore")

def My_YM(df, date_col):
    """
    把日期列（形如 'YYYY-MM-DD' 或 'YYYYMMDD'）转换到 Yearmon 列
    """
    df['DATE'] = (
        df[date_col]
        .astype(str)
        .str.replace(r'\D', '', regex=True)
        .astype(int)
    )
    df['Yearmon'] = df['DATE'] // 100

def main():
    # 1. 原始 Excel 文件所在路径
    in_dir = r"/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor_data_storage/日个股回报率/第二组(25)"
    # 2. 输出 CSV 路径
    out_path = r"/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/volatility/volatility_2.csv"

    # —— 读取并合并所有 Excel 文件 ——
    files = [f for f in glob.glob(os.path.join(in_dir, "*.xlsx")) if not os.path.basename(f).startswith("~$")]
    raw = []
    for f in files:
        df = pd.read_excel(f, dtype=str)
        raw.append(df)
        print(f"{os.path.basename(f)} 合并完成")
    d = pd.concat(raw, ignore_index=True)

    # —— 数据清洗 ——
    # 去除多余的表头行（如“证券代码”或“没有单位”），并把缺失值填 0
    d = d[~d['Stkcd'].isin(["证券代码", "没有单位"])].copy()
    d = d.fillna("0")

    # —— 取所需列并生成 Yearmon ——
    RET = d[['Stkcd', 'Trddt', 'Dretwd']].copy()
    My_YM(RET, 'Trddt')

    # —— 分组计算月度收益率标准差 ——
    RETS = (
        RET
        .groupby(['Stkcd', 'Yearmon'])['Dretwd']
        .apply(lambda x: x.astype(float).std())
        .reset_index()
        .rename(columns={'Dretwd': 'volatility'})
    )

    # —— 向下取上期值 ——
    RETS['volatility'] = RETS.groupby('Stkcd')['volatility'].shift(1)

    # —— 丢弃 NA 并保存 ——
    RETS = RETS.dropna(subset=['volatility']).reset_index(drop=True)
    RETS[['Stkcd', 'Yearmon', 'volatility']].to_csv(
        out_path,
        encoding='utf_8_sig',
        index=False
    )

    print(f"波动率因子已保存到：{out_path}")

if __name__ == "__main__":
    main()
