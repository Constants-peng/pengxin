# 该模块封装 函数
import pymysql
import re
from sqlalchemy import create_engine
import pandas as pd
import time
import numpy as np
import threading
from queue import Queue
# import ipynb_importer
from get_1688.config import get_mysql_cfg, get_table_cfg, cfg


createVar, timeday, sql_queue = locals(), time.localtime(), Queue()


def main(mysql_cfg: str, table_source: str):
    # 全局变量名,时间元组
    engine = create_engine(get_mysql_cfg(mysql_cfg))  # 链接数据库
    table_cfg_dict = get_table_cfg(table_source)
    for table_name, field_list in table_cfg_dict.items():
        if len(field_list):
            fields = ",".join(field_list)
        else:
            fields = "*"
        sql = "select {} from {}".format(fields, table_name)
        sql_queue.put((sql, table_name))
    thread_list = []
    for i in range(4):
        thread = threading.Thread(target=read_mysql_table, args=(engine,))
        thread_list.append(thread)
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()


# 读取mysql数据
def read_mysql_table(engine):
    while True:
        if sql_queue.empty():
            return
        try:
            sql, table_name = sql_queue.get()
            df = pd.read_sql_query(sql, engine)
            df = need_dtype(df)
            createVar["df_"+table_name] = df
        except Exception as e:
            print("%s 查询错误: %s" % (table_name, str(e)))
            continue

# 转换dataframe数据类型,可以明确指定那些列不需要转换为指定的类型,首先将dataframe中所有可以转为数值数据的转为数字类型,其他转为时间类型


def need_dtype(df: pd.core.frame.DataFrame):
    # 此处应该加逻辑 1.指定那些列需要不能转为数字类型而应该转为 时间类型
    for col_name in df.columns:
        df[col_name] = df[col_name].where(df[col_name].apply(lambda order_name: False if re.match(
            r'(\s+$)|(nan$)', str(order_name), re.I) else True), np.nan)
        try:
            # 匹配时间
            flag = 'flag'
            df[flag] = df[col_name].apply(lambda order_name: True if re.match(
                r'((19[7-9]\d{1})|((201[0-9])|202[0-2]))[-/]?((0[1-9])|(1[0-2]))[-/]?((0[1-9])|(1[0-9])|(2[0-9])|(3[0-1]))?.*?', str(order_name), re.I) else False)
            if df[df[flag]].shape[0] == df.shape[0] * 0.6:   #数据中有3/5的数据是类似时间类型,将其转为时间类型
                df[col_name] = pd.to_datetime(df[col_name], errors='ignore')
                df.drop(columns=[flag], inplace=True)
                continue
            df[col_name] = pd.to_numeric(
                df[col_name], errors='ignore')  # ignore 无效的解析将返回输入
            df.drop(columns=[flag], inplace=True)
        except:
            continue
    return df


# 获取星级计算
def get_star(x):
    if x['月下单用户数'] >= 50 and x['nmv'] >= 10000 and x['生鲜件数占比'] >= 0.5 and x['月营业天数'] >= (timeday[2]-2):
        return 5
    elif x['月下单用户数'] >= 25 and x['nmv'] >= 3750 and x['月营业天数'] >= (timeday[2]-5):
        return 4
    elif x['月下单用户数'] >= 10 and x['nmv'] >= 1000:
        return 3
    elif x['月下单用户数'] >= 3 and x['nmv'] >= 200:
        return 2
    else:
        return 1



if __name__ == "__main__":
    print(timeday)
    #     df = transpose_df(df, by_name='蜂窝')
#     main("test", "table_pass")
#     df = pd.read_excel(r'E:\钉钉excel文件\cms262.xlsx')