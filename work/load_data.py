import pandas as pd
import re
import numpy as np
from typing import List, Iterable
import os
import pymysql
from sqlalchemy import create_engine,exc

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
                    df[col_name].apply(lambda x: str(x)), errors='coerce')
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


def read_file(file_path: Iterable):
    # 所有的表都以append方式入库,不允许以replace的方式入库
    for file in file_path:
        df = pd.read_excel(file)
        df = dropna_col(df)
        df = need_dtype(df)
        print(file)
        try:               #此处建议用pymysql来操作插入语句
            # if "day" in file:
            #     df.to_sql("tz_day_sale", con=con, if_exists="append", index=False)
            # elif "cms" in file:
            #     df.drop_duplicates(subset='ID').to_sql('cms_tz', con=con, if_exists='append', index=False)  # 将当前对象存入数据库
            if "month" in file:
                df.drop_duplicates(subset='团长用户ID').to_sql("tz_month_base", con=con, if_exists="append", index=False)
            elif "message" in file:
                df.drop_duplicates(subset='团长id').to_sql("tz_message", con=con, if_exists="append", index=False)
        except exc.IntegrityError as e:
            print(str(e))
 # 将目录下的文件名添加到file_list列表中
def file_name_walk(file_dir):
    file_list = []
    for root, dirs, files in os.walk(file_dir):
        for file_name in files:
            file_list.append(os.path.join(root, file_name))
    return file_list


if __name__ == "__main__":
    truncate_table(['work_data.tz_month_sale', 'work_data.tz_message',"work_data.tz_day_sale"])  # 传入一个列表,为了不改变原表数据结构,选择清空表
    read_file(file_name_walk(r"E:\钉钉excel文件\0707"))