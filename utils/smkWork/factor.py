import pandas as pd
import numpy as np
import os
import warnings
from pathlib import Path
warnings.filterwarnings('ignore')
os.chdir("../../")

# Data Processing
def process_old_data(path='./data/factor/dolvol_turn_zerotrade', start_str='LT'):
    df_list = []
    base = Path(path)
    for file in base.rglob('*.xlsx'):
        if not file.is_file():
            continue

        rel_parts=file.relative_to(base).parts[:-1]

        if any('N' in part for part in rel_parts):
            continue

        if not file.name.startswith(start_str):
            continue

        df = pd.read_excel(file)
        df_list.append(df)

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
    else: 
        combined_df = pd.DataFrame()

    df_ = combined_df[combined_df['Stkcd']!= "证券代码"]
    df_ = df_[df_['Stkcd']!= "没有单位"].reset_index(drop=True)
    df_ = df_.fillna(0)
    df_.to_stata('data/factor/dolvol_turn_zerotrade/DailyTrade.dta')
    

    df_ = df_.rename(
        columns = {'Trddt': 'Date',
                'Prccls': 'Price',
                'Tolstknum': 'Volume',
                'Tolstknva': 'Amount'})
    
    return df_

# Convert date to year and month
def My_yearmon(Set,YMD):
    Set[YMD] = pd.to_datetime(Set[YMD],format='%Y-%m-%d')
    Set['year'] = Set[YMD].dt.year
    Set['month'] = Set[YMD].dt.month

# Trading factor
def _data_process():
    path = './data/factor/dolvol_turn_zerotrade'
    start_str = 'LT'

    df = process_old_data(path, start_str)

    TRDC = pd.read_excel('./data/factor/dolvol_turn_zerotrade/股本变动文件旧版/TRD_Capchg.xlsx') 
    TRDC = TRDC.iloc[2:]
    
    My_yearmon(TRDC,'Shrchgdt')
    My_yearmon(df,'Date')

    df['DATE'] = df['Date'].astype(str).replace('-', '', regex=True)
    df['Yearmon'] = df['DATE'].astype(int) // 100

    TRDC['DATE'] = TRDC['Shrchgdt'].astype(str).replace('-', '', regex=True)
    TRDC['Yearmon'] = TRDC['DATE'].astype(int) // 100

    TRDCM = TRDC[['Stkcd','Yearmon','Nshra']].drop_duplicates(subset = ['Stkcd','Yearmon'], keep = 'last').reset_index(drop=True)

    
    return df, TRDCM

def _construct_month_list():

    df, TRDCM = _data_process()

    from itertools import product 
    # 月份编号
    Mon_list = np.unique(TRDCM['Yearmon'])
    R2 = pd.DataFrame(columns=['Yearmon','ID_Mon'],
                        index=np.arange(len(Mon_list))
                        )
    R2['Yearmon'] = Mon_list
    R2['ID_Mon'] = np.arange(len(Mon_list)) 

    TRDCM = pd.merge(TRDCM,R2,on='Yearmon',how='left')
    df = pd.merge(df,R2,on='Yearmon',how='left')

    Stkcd_list = np.unique(TRDCM['Stkcd'])

    Mon_L1 = []
    Mon_L2 = []

    for m1, m2 in product(Stkcd_list, Mon_list):
        Mon_L1.append(m1)
        Mon_L2.append(m2)

    R1 = pd.DataFrame(columns=['Stkcd','Yearmon'],
                      index=np.arange(len(Mon_L1)))
    
    R1['Stkcd'] = Mon_L1
    R1['Yearmon'] = Mon_L2
    R1 = pd.merge(R1,R2,on='Yearmon',how='left')

    return R1, df, TRDCM

def dolvol():
    '''
    :frequency: Monthly
    :calculation: t-2 month trading total amount times stock price and logrithm
    '''
    R1, df, TRDCM = _construct_month_list()

    Amount = df[['Stkcd','ID_Mon','Amount']]
    Amount['dolvol'] = np.log(Amount['Amount'])
    Amount['ID_Mon_2'] = Amount['ID_Mon'] + 2

    return Amount

def turn():

    R1, df, TRDCM = _construct_month_list()

    Amount = dolvol()

    R1_ = pd.merge(R1,Amount[['Stkcd','ID_Mon_2','dolvol']],
              left_on = ['Stkcd','ID_Mon'],
              right_on = ['Stkcd','ID_Mon_2'],
              how='left')

    VL = df[['Stkcd','ID_Mon', 'Volume']
        ].groupby(['Stkcd','ID_Mon']
         ).sum().reset_index()
    VL['ID_Mon_1'] = VL['ID_Mon']+1
    VL['ID_Mon_2'] = VL['ID_Mon']+2
    VL['ID_Mon_3'] = VL['ID_Mon']+3

    n = 1
    for i in ['ID_Mon_1', 'ID_Mon_2', 'ID_Mon_3']:
        R1_=pd.merge(R1_, VL[['Stkcd', i, 'Volume']],
                     left_on = ['Stkcd', 'ID_Mon'],
                     right_on = ['Stkcd', i],
                     how = 'left')
        
        R1_ = R1_.rename(
            columns = {
                'Volume': 'Volume_' + str(n)
            }
        )

        n = n+1 

    R1_ = pd.merge(R1_,TRDCM[['Stkcd', 'Yearmon', 'Nshra']],on=['Stkcd','Yearmon'],how='left')

    R1_ = R1_.fillna(0)

    R1_['turn'] = (R1_['Volume_1']+R1_['Volume_2']+R1_['Volume_3'])/R1_['Nshra']

    R1_['turn'] = R1_['turn'].fillna(0)

    return R1_

def zeroturn():
    '''
    :frequency: Monthly
    :calcultion: t-1 month 0 trading amount
    '''
    
    _, df, TRDCM_ = _construct_month_list()

    Zeroturn = df[['Stkcd','ID_Mon', 'Volume']]
    Zeroturn['zero'] = np.where(Zeroturn['Volume'] == 0,1,0)

    Zeroturn.groupby('zero')['zero'].count()


    Zeroturn = Zeroturn[['Stkcd','ID_Mon', 'zero']
            ].groupby(['Stkcd','ID_Mon']
             ).count().reset_index()
    Zeroturn['ID_Mon_1'] = Zeroturn['ID_Mon']+1  

    return Zeroturn

def std_dolvol():

    '''
    :frequency: Monthly
    '''

    _R1, df, _TRDCM = _construct_month_list()
    R1_ = turn()
    zero = zeroturn()

    R1_mid = pd.merge(R1_, zero[['Stkcd', 'ID_Mon_1', 'zero']],
                    left_on = ['Stkcd', 'ID_Mon'],
                    right_on = ['Stkcd', 'ID_Mon_1'],
                    how = 'left')
    
    Std_Dol = df[['Stkcd', 'ID_Mon', 'Volume']
                 ].groupby(['Stkcd', 'ID_Mon']).std().reset_index()
    
    Std_Dol = Std_Dol.rename(
        colums = {'Volume': 'std_dolvol'}
    )

    return Std_Dol

def std_turn():

    _R1, df, _TRDCM = _construct_month_list()

    std_turn = df[['Stkcd', 'ID_Mon', 'Amount']
                  ].groupby(['Stkcd', 'ID_Mon']).std().reset_index()

    R1_ = turn()

    zero = zeroturn()

    R1_mid = pd.merge(R1_, zero[['Stkcd', 'ID_Mon_1', 'zero']],
                    left_on = ['Stkcd', 'ID_Mon'],
                    right_on = ['Stkcd', 'ID_Mon_1'],
                    how = 'left')
    
    R1_mid = pd.merge(R1_mid, std_turn[['Stkcd', 'ID_Mon', 'std_turn']],
                      left_on = ['Stkcd', 'ID_Mon'],
                      right_on = ['Stkcd', 'ID_Mon'],
                      how = 'left')
    
    R1_mid = R1_mid.replace([np.inf, -np.inf], np.nan)

    R1_mid = R1_mid.fillna(0)

    return R1_mid

def mixture():
    
    R1_mid = std_turn()[['Stkcd', 'ID_Mon', 'std-turn']]

    R1_mid = R1_mid.merge(
        dolvol()[['Stkcd', 'ID_Mon', 'dolvol']],
        on = ['Stkcd', 'ID_Mon'], how = 'left'
    ).merge(
        turn()[['Stkcd', 'ID_Mon', 'turn']],
        on = ['Stkcd', 'ID_Mon'], how = 'left'
    ).merge(
        zeroturn()[['Stkcd', 'ID_Mon', 'zero']],
        on = ['Stkcd', 'ID_Mon'], how = 'left'
    ).merge(
        std_dolvol()[['Stkcd', 'ID_Mon', 'std_dolvol']],
        on = ['Stkcd', 'ID_Mon'], how = 'left'
    )

    R1_mid = R1_mid.fillna(0)
    R1_mid['flag'] = (
        R1_mid['dolvol']
        + R1_mid['turn']
        + R1_mid['zero']
        + R1_mid['std_dolvol']
        + R1_mid['std_turn']
    )

    return R1_mid

if __name__ == "__main__":

    FinalTable = mixture()
    FinalTable = FinalTable[FinalTable['flag'] != 0.0].reset_index(drop=True)

    print(FinalTable[['Stkcd', 'Yearmon','dolvol','turn','zero','std_dolvol','std_turn']])

    FinalTable[['Stkcd', 'Yearmon','dolvol','turn','zero','std_dolvol','std_turn']].to_csv(
    "TRD_dolvol_treated.csv",encoding='utf_8_sig',index = False)
