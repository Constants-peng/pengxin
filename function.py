# 该模块封装 函数
import pymysql
import re
from sqlalchemy import create_engine
import pandas as pd
import time
import ipynb_importer
from config import get_mysql_cfg, get_table_cfg
from typing import List


df_list = []

# 获取当前时间元组


def get_date_tuple():
    localtime = time.localtime()
    return localtime[0], localtime[1], localtime[2]

# 读取mysql数据


def read_mysql_table(mysql_cfg: str):
    table_cfg_dict = get_table_cfg()  # 返回需要查询的表以及字段,字典类型
    for table_name, field_list in table_cfg_dict.items():
        fields = ",".join(field_list)
        sql = "select {} from {}".format(fields, table_name)
        try:
            engine = create_engine(get_mysql_cfg(mysql_cfg))  # 链接数据库
            df = pd.read_sql_query(sql, engine)
            df_list.append(df)
        except:
            print("%s 数据表查询有误" % table_name)
            continue


def need_dtype(df: pd.core.frame.DataFrame):  # df对象中存在空白字符的可能性,需要替换为nan
    for col_name in df.columns:
        try:
            df[col_name] = df[col_name].where(df[col_name].apply(
                lambda order_name: False if re.match(r'(\s+$)|(nan$)', order_name, re.I) else True), np.nan)
            # 查看str类型是否能首先转为时间类型 如果能则选择转为时间类型, 否则转为数值类型
            df[col_name] = pd.to_datetime(
                df[col_name], errors='ignore')
            if df[col_name].dtypes == 'datetime64[ns]':  # 时间类型
                continue
            df[col_name] = pd.to_numeric(
                df[col_name], errors='ignore')  # ignore 无效的解析将返回输入
        except:
            continue
    return df

def get_week(x):
    # x = pd.to_datetime(x,format="%Y-%m-%d")
    x = time.strptime(str(x).split(" ")[0], "%Y-%m-%d")
    y = x.tm_year
    m = x.tm_mon
    d = x.tm_mday
    return datetime.datetime(int(y), int(m), int(d)).isocalendar()[1]

# 获取星级计算

def get_star(x):
    if x['月下单用户数'] >= 50 and x['nmv'] >= 10000 and x['生鲜件数占比'] >= 0.5 and x['月营业天数'] >= (date[1]-2):
        star_list.append(5)
        return
    elif x['月下单用户数'] >= 25 and x['nmv'] >= 3750 and x['月营业天数'] >= (date[1]-5):
        star_list.append(4)
        return
    elif x['月下单用户数'] >= 10 and x['nmv'] >= 1000:
        star_list.append(3)
        return
    elif x['月下单用户数'] >= 3 and x['nmv'] >= 200:
        star_list.append(2)
        return
    else:
        star_list.append(1)