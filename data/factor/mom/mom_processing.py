"""
累计收益因子处理
"""
import numpy as np
import pandas as pd
import os
import warnings
from itertools import product
import glob

warnings.filterwarnings("ignore")

def main():
    # 设置路径
    path = r"/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor_data_storage/日个股回报率/第一组(00)"
    os.chdir(path)
    file_name_list = [f for f in glob.glob("*.xlsx") if not f.startswith("~$")]


    # 读入、合并文件
    d = pd.DataFrame()
    for fname in file_name_list:
        m = pd.read_excel(fname)
        d = pd.concat([d, m]).reset_index(drop=True)
        print(f"{fname} Completed")

    # 数据清洗
    d_ = d[(d['Stkcd'] != "证券代码") & (d['Stkcd'] != "没有单位")].reset_index(drop=True)
    d_ = d_.fillna(0)
    d_.to_stata('/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/mom1m/DailyRet_1.dta')

    # 构建 RET 数据框
    RET = d_[['Stkcd', 'Trddt', 'Dretnd']].rename(columns={'Dretnd': 'RET'})
    RET['Trddt'] = pd.to_datetime(RET['Trddt'], format='%Y-%m-%d')
    RET['year'] = RET['Trddt'].dt.year
    RET['month'] = RET['Trddt'].dt.month
    RET['DATE'] = RET['Trddt'].dt.strftime('%Y%m%d').astype(int)
    RET['Yearmon'] = RET['DATE'] // 100

    # 月份编号映射
    Mon_list = np.unique(RET['Yearmon'])
    R2 = pd.DataFrame({'Yearmon': Mon_list, 'ID_Mon': np.arange(len(Mon_list))})
    RET_ = pd.merge(RET, R2, on='Yearmon', how='left')

    # 构建股票-月索引表格
    Stkcd_list = np.unique(RET_['Stkcd'])
    X1, X2 = zip(*product(Stkcd_list, Mon_list))
    R1 = pd.DataFrame({'Stkcd': X1, 'Yearmon': X2})
    R1 = pd.merge(R1, R2, on='Yearmon', how='left')
    R1['ID_Mon_m1'] = R1['ID_Mon'] - 1

    # 计算 maxret
    maxret = RET_.groupby(['Stkcd','ID_Mon'])['RET'].max().reset_index().rename(columns={'RET':'maxret'})
    R1 = pd.merge(R1, maxret, left_on=['Stkcd','ID_Mon_m1'], right_on=['Stkcd','ID_Mon'], how='left')
    R1 = R1.rename(columns={'ID_Mon_x':'ID_Mon'}).drop(columns=['ID_Mon_y'])

    # 计算 mom1m
    RETM = RET_.groupby(['Stkcd','ID_Mon'])['RET'].sum().reset_index().rename(columns={'RET':'mom1m'})
    R1 = pd.merge(R1, RETM, on=['Stkcd','ID_Mon'], how='left')

    # 计算 mom6m
    M6 = RETM.groupby('Stkcd')['mom1m'].rolling(window=5).sum().reset_index().rename(columns={'mom1m':'mom6m'})
    M6 = M6.drop(columns='level_1')
    M6_ = pd.concat([M6, RETM['ID_Mon']], axis=1)
    R1 = pd.merge(R1, M6_, left_on=['Stkcd','ID_Mon_m1'], right_on=['Stkcd','ID_Mon'], how='left')
    R1 = R1.rename(columns={'ID_Mon_x':'ID_Mon'}).drop(columns=['ID_Mon_y'])

    # 计算 mom12m
    M12 = RETM.groupby('Stkcd')['mom1m'].rolling(window=11).sum().reset_index().rename(columns={'mom1m':'mom12m'})
    M12 = M12.drop(columns='level_1')
    M12_ = pd.concat([M12, RETM['ID_Mon']], axis=1)
    R1 = pd.merge(R1, M12_, left_on=['Stkcd','ID_Mon_m1'], right_on=['Stkcd','ID_Mon'], how='left')
    R1 = R1.rename(columns={'ID_Mon_x':'ID_Mon'}).drop(columns=['ID_Mon_y'])

    # 计算 mom36m（通过 36 期和 12 期差值）
    T36 = RETM.groupby('Stkcd')['mom1m'].rolling(window=36).sum().reset_index().rename(columns={'mom1m':'T36'})
    T36 = T36.drop(columns='level_1')
    T36_ = pd.concat([T36, RETM['ID_Mon']], axis=1)
    R1 = pd.merge(R1, T36_, left_on=['Stkcd','ID_Mon_m1'], right_on=['Stkcd','ID_Mon'], how='left')
    R1 = R1.rename(columns={'ID_Mon_x':'ID_Mon'}).drop(columns=['ID_Mon_y'])

    T12 = RETM.groupby('Stkcd')['mom1m'].rolling(window=12).sum().reset_index().rename(columns={'mom1m':'T12'})
    T12 = T12.drop(columns='level_1')
    T12_ = pd.concat([T12, RETM['ID_Mon']], axis=1)
    R1 = pd.merge(R1, T12_, left_on=['Stkcd','ID_Mon_m1'], right_on=['Stkcd','ID_Mon'], how='left')
    R1 = R1.rename(columns={'ID_Mon_x':'ID_Mon'}).drop(columns=['ID_Mon_y'])

    R1['mom36m'] = R1['T36'] - R1['T12']

    # 最终结果保存
    R1_ = R1[['Stkcd','Yearmon','maxret','mom1m','mom6m','mom12m','mom36m']]
    R1_ = R1_.loc[R1_['mom1m'] > -9999].reset_index(drop=True)
    R1_.to_csv("/Users/tongchen/Desktop/SFML/2025Spring_Financial_ML/data/factor/mom1m/MOM_treated_1.csv", encoding='utf_8_sig', index=False)

if __name__ == "__main__":
    main()
