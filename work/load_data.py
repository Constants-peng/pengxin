import pandas as pd
import re
import numpy as np
from typing import List
import pymysql
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)
con = create_engine('mysql+pymysql://pengxin:123456@localhost:3306/work_data?charset=utf8mb4')


def dropna_col(df: pd.core.frame.DataFrame):
    for col in df.columns:
        if df[col].isnull().sum() > df.shape[0] * 0.6:
            # 缺失值列大于原有df对象所有行的0.6认为该列删除
            df.drop(columns=col, inplace=True)
    return df


pd.set_option('display.max_columns', None)


def need_dtype(df: pd.core.frame.DataFrame):
    # 此处应该加逻辑 1.指定那些列需要不能转为数字类型而应该转为 时间类型
    for col_name in df.columns:
        df[col_name] = df[col_name].where(df[col_name].apply(lambda order_name: False if re.match(
            r'(\s+$)|(nan$)', str(order_name), re.I) else True), np.nan)
        try:
            # 匹配时间
            flag = 'flag'
            df[flag] = df[col_name].apply(lambda order_name: True if re.match(
                r'((19[7-9]\d{1})|((201[0-9])|202[0-2]))[-/]?((0[1-9])|(1[0-2]))[-/]?((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))?', str(order_name), re.I) else False)
            if df[df[flag]].shape[0] > df.shape[0] * 0.6:
                df[col_name] = pd.to_datetime(
                    df[col_name].apply(lambda x: str(x)), errors='ignore')
                df.drop(columns=[flag], inplace=True)
                continue
            df[col_name] = pd.to_numeric(
                df[col_name], errors='ignore')  # ignore 无效的解析将返回输入
            df.drop(columns=[flag], inplace=True)
        except:
            continue
    return df


def truncate_table(db_table_list: List):
    db = pymysql.connect(host='127.0.0.1', user='pengxin',
                         passwd='123456', charset='utf8mb4')
    cursor = db.cursor()
    # 清空stock_tmp
    for table in db_table_list:
        try:
            query = "truncate table %s" % table
            cursor.execute(query)
            db.commit()
            print('{}原表已清空'.format(table))
        except Exception as e:
            print('无{}表！'.format(table))
    db.close()


truncate_table(['work_data.tz_month_sale', 'work_data.tz_base', 'work_data.tz_day_sale'])  # 传入一个列表,为了不改变原表数据结构,选择清空表
# 所有的表都以append方式入库,不允许以replace的方式入库
df_cms_tz = pd.read_excel(r"E:\钉钉excel文件\0706\tz_base_20210706_14_55_05.xlsx")  # 读取cms表
cms_tz_temp = dropna_col(df_cms_tz)
cms_tz_temp = need_dtype(cms_tz_temp)
cms_tz_temp.drop_duplicates(subset='ID').to_sql('cms_tz', con=con, if_exists='append', index=False)  # 将当前对象存入数据库

df_tz_month_base = pd.read_excel(r"E:\钉钉excel文件\0706\tz_base_20210706_14_55_05.xlsx")  # 读取团长销售月报
tz_base = need_dtype(dropna_col(df_tz_month_base))
tz_base.drop_duplicates(subset='团长用户ID').to_sql("tz_base", con=con, if_exists="append", index=False)

df_tz_day_sale = pd.read_excel(r"E:\钉钉excel文件\0706\tz_day_20210706_15_07_11.xlsx")  # 读取团长日报数据
tz_day_sale = need_dtype(dropna_col(df_tz_day_sale))
tz_day_sale.to_sql("tz_day_sale", con=con, if_exists="append", index=False)  # 所有的表都以append方式入库,不允许以replace的方式入库
