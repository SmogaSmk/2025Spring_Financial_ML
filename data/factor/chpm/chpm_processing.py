#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd

# 1. 读取 Excel，第一行为列名，跳过第二行的单位说明
input_path = 'data/factor_data_storage/盈利能力/第二组(25)/FI_T5.xlsx'
FIT5 = pd.read_excel(
    input_path,
    header=0,
    skiprows=[1],
    dtype={'Stkcd': str}
)

# 2. 去掉列名两端空格（防止匹配不到）
FIT5.columns = FIT5.columns.str.strip()

# （可选）打印列名，确认哪些列存在
# print(">>> 列名列表:", FIT5.columns.tolist())

# 3. 重命名我们关心的字段
FIT5 = FIT5.rename(columns={
    'F050601B': 'EBIT',    # 息税前利润
    'F051101B': 'EBITTA',  # EBITTA 用作示例中的总资产
    # 如果有其它需要重命名的字段，这里继续添加
})

# 4. 强制把关键列转成数值型，否则后续运算会报错
FIT5['EBIT']   = pd.to_numeric(FIT5['EBIT'], errors='coerce')
FIT5['EBITTA'] = pd.to_numeric(FIT5['EBITTA'], errors='coerce')

# 5. 丢掉任何在 Stkcd、Accper、EBIT、EBITTA 上有缺失的行
FIT5 = FIT5.dropna(subset=['Stkcd', 'Accper', 'EBIT', 'EBITTA'])

# 6. 按股票分组计算上一期 EBIT
FIT5['EBIT_lag1'] = FIT5.groupby('Stkcd')['EBIT'].shift(1)

# 7. 把 EBITTA 当作总资产 TA
FIT5['TA'] = FIT5['EBITTA']

# 8. 只保留上一期 EBIT 存在且 TA>0 的数据
FIT5 = FIT5[(FIT5['EBIT_lag1'].notna()) & (FIT5['TA'] > 0)]

# 9. 根据定义计算 chpm 因子
FIT5['chpm'] = (FIT5['EBIT'] - FIT5['EBIT_lag1']) / FIT5['TA']

# 10. 生成最终结果表，只保留 Stkcd、Accper、chpm 三列
result = FIT5[['Stkcd', 'Accper', 'chpm']].dropna()

# 11. 确保输出目录存在
out_dir = 'data/factor/chpm'
os.makedirs(out_dir, exist_ok=True)

# 12. 写入 CSV
out_path = os.path.join(out_dir, 'chpm_2.csv')
result.to_csv(
    out_path,
    index=False,
    encoding='utf_8_sig'
)

print(f"已生成 chpm 因子文件，共 {len(result)} 条记录，路径：{out_path}")
