from typing import List
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engin = create_engine("mysql+pymysql://pengxin:123456@localhost:3306/work_data")
df_tz_base = pd.read_sql_query("select 城市圈,蜂窝 from tz_base", con=engin)

mean_list = ['下单用户数', '件数', 'UV访问量', '人均购买件数',
             '单团用户数', '团效-件数', '动销团长数']  # 这些指标求mean 其他指标求和


def file_name_walk(file_dir):  # 传入文件目录
    df_result = pd.DataFrame()
    for root, dirs, files in os.walk(file_dir):
        for file_name in files:
            df = pd.read_excel(os.path.join(
                root, file_name), sheet_name='Sheet1')
            df = df.dropna(axis=0, how='any', subset=['蜂窝', 'TC仓'])
            merge_list = ['蜂窝', 'TC仓', '指标']  # 这段代码对象中有城市圈字段  此处可以加入城市圈到列表中
            if df_result.empty:
                print("df_result对象为空 赋值操作")
                df_result = df
            else:
                df_result = pd.merge(
                    df_result, df, left_on=merge_list, right_on=merge_list, how='left')
                # 这段代码期间可以省略 前提是 df_result对象中有城市圈字段  此处可以加入判断
    # 去重匹配 df_result ,df_result 获取城市圈字段
    tz_base = df_tz_base.drop_duplicates(subset='蜂窝')
    df_result = pd.merge(tz_base, df_result, left_on='蜂窝', right_on='蜂窝')
    df_result = df_result[(df_result['TC仓'] != '8屏南') | (df_result['TC仓'] != '3柘荣') | (
        df_result['TC仓'] != '24鉴江') | (df_result['TC仓'] != '郭坑TC')]
    print(df_result[df_result['TC仓'] == '3柘荣'])
    global df_data
    #   首先解析 城市圈看板 福州和漳州两个城市圈看板(两个文件流) 按照城市圈和蜂窝和指标分组求各项指标 ,city_list分组对象

    #     bc_list TC看板

    tc_list, city_list = ['TC仓', '指标'], ['城市圈', '蜂窝', '指标']
    df_city = cal(df_result, city_list).drop(columns='TC仓')  # 城市与蜂窝 为主  TC仓去除
    df_tc = cal(df_result, tc_list)  # tc为主
    # 创建两个写入流   写入福州看板与漳州看板
    fuzhou_writer = pd.ExcelWriter(
        r'E:\钉钉excel文件\result\福州蜂窝看板.xlsx')  # 福州蜂窝写入流
    zhangzhou_writer = pd.ExcelWriter(
        r'E:\钉钉excel文件\result\漳州蜂窝看板.xlsx')  # 漳州蜂窝写入流
    tc_list, city_list = ['城市圈', '蜂窝'], ['城市圈']
    for group_name, df in df_city.groupby(by=city_list):
        result_city = []
        for index_name, df_index in df.groupby(by=['指标']):
            df_cal = df_index.loc[:, '总计':]
            index_list = []  # 储存原行数据
            index_list.append(group_name)
            if index_name in mean_list:
                index_list.append(index_name)
                index_series = df_cal.apply(np.mean)
                index_list.extend(index_series.to_list())
            else:
                index_list.append(index_name)
                index_series = df_cal.apply(np.sum)
                index_list.extend(index_series.to_list())
            result_city.append(index_list)
        df_temp = pd.DataFrame(result_city)
        global city_df
        city_df = df.drop(columns=['城市圈'])
        df_temp.columns = city_df.columns
        city_df = pd.concat([df_temp, city_df])
        city_df.loc[:, '总计':].apply(number_astype_city)
        city_df.set_index(["蜂窝", "指标"], inplace=True)  # 重置索引 为下次遍历每一列处理数字做准备

        if group_name == '漳州城市圈':  # 当前df对象使用漳州蜂窝写入流
            city_df.to_excel(zhangzhou_writer, sheet_name=group_name)
        else:  # 当前df对象使用福州蜂窝写入流
            city_df.to_excel(fuzhou_writer, sheet_name=group_name)
    for group_name, df in df_tc.groupby(by=tc_list):
        df.set_index(["TC仓", "指标"], inplace=True)  # 重置索引 为下次遍历每一列处理数字做准备
        if group_name[0] == "福州城市圈":
            df.drop(columns=tc_list).to_excel(fuzhou_writer, sheet_name=group_name[1])
        else:
            df.drop(columns=tc_list).to_excel(zhangzhou_writer, sheet_name=group_name[1])
    for i in [fuzhou_writer, zhangzhou_writer]:
        i.save()
        i.close()


def cal(df_result: pd.core.frame.DataFrame, by_list: List):
    global df_data
    df_data, count = pd.DataFrame(), 1  # df_data返回对象 , count控制行
    for group_name, df in df_result.groupby(by=by_list):  # 按照 值
        df_by = df.drop(columns=by_list)
        if group_name[1] == 'GMV':  # 定义不同的指标,计算的方式不同  需要用continue关键字
            gmv_mean = df_by.mean().values[0]

        for index in range(len(by_list)):  # 填充新对象单元格
            df_data.loc[count, by_list[index]] = group_name[index]
        index_temp = 0
        for index in df_by.columns:  # 遍历日期 将每一天的计算结果sum 填入单元格
            df_data.loc[count, index] = df_by.sum().values[index_temp]
            index_temp = index_temp + 1
        count = count + 1
    df_date = df_data.drop(columns=by_list).sort_index(
        ascending=False, axis=1)  # 按照时间排序
    df_data = pd.concat([df_data.loc[:, by_list], df_date], axis=1)
    df_sum_mean = df_data.apply(sum_mean, axis=1)  # 遍历每一行,对不同指标求和 与均值
    df_data.insert(4, '总计', df_sum_mean)
    if by_list == ['城市圈', '蜂窝', '指标']:  # 城市圈看板还要在计算一个
        return df_data
    df_data.iloc[:, 4:].apply(number_astype)
    return df_data


def number_astype_city(x):
    global city_df
    x = x.map(lambda x: format(int(round(x, 0)), ',')
    if x > 1000 else round(x, 1))
    city_df[x.to_frame().columns.values[0]] = x


def sum_mean(x):
    drop_list = ['城市圈', '蜂窝', 'TC仓', '指标']
    if x['指标'] in mean_list:
        return x.drop(drop_list).mean()
    return x.drop(drop_list).sum()


def number_astype(x):  # 处理数值型精度问题  此处必须在确定以及将dataframe对象数据聚合操作全部处理完毕
    global df_data
    x = x.map(lambda x: format(int(round(x, 0)), ',')
    if x > 1000 else round(x, 1))
    df_data[x.to_frame().columns.values[0]] = x


if __name__ == "__main__":
    file_name_walk(r'E:\钉钉excel文件\蜂窝')
