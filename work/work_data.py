from typing import Tuple,List
from collections import Iterable
import datetime
from get_1688.custom_function import *

# 全局变量分化 此处可以修改全局变量的值 请不要直接修改或者删除createVar 变量
df_tz_day_sale, df_cms_tz, df_tz_base, df_tz_month_sale, df_city_station = (
    pd.DataFrame() for i in range(5))


# 连接键重命名


def source_data():
    global df_tz_day_sale, df_cms_tz, df_tz_base, df_tz_month_sale, df_city_station
    tz_id = "团长ID"
    for key in createVar.keys():
        if key == "df_tz_day_sale":
            df_tz_day_sale = createVar[key].rename(
                columns={"团长id": tz_id})
        elif key == "df_cms_tz":
            df_cms_tz = createVar[key].rename(columns={"ID": tz_id})
        elif key == "df_tz_base":
            df_tz_base = createVar[key].rename(
                columns={'城市圈': '蜂窝城市圈'})
        elif key == "df_tz_month_sale":
            df_tz_month_sale = createVar[key].rename(columns={"团长用户ID": tz_id})
        elif key == "df_city_station":
            df_city_station = createVar[key]
        else:
            continue


            # 转置df对象


def transpose_df(df: pd.core.frame.DataFrame, by_name: [List, str]):
    transpose_df_list = []
    for group_name, df_group in df.groupby(by=by_name):
        df_group = df_group.drop(columns=by_name).T
        by_tc = "TC仓"
        for col in df_group.columns:  # 遍历df_group 中每一列,获取列的名称
            df_col = df_group.loc[:, [col]]
            tc_name, df_tc = df_col.loc[by_tc, col], df_col.drop(
                index=by_tc)  # 将当前TC仓的值取出,减少操作
            df_tc.columns = ["{}日".format(timeday[2])]
            df_tc[by_name[0]], df_tc[by_name[1]
            ], df_tc[by_tc], df_tc['指标'] = group_name[0], group_name[1], tc_name, df_tc.index
            # 设置 蜂窝,TC仓为索引
            df_tc.set_index([by_name[0], by_name[1], by_tc], inplace=True,
                            drop=False)  # 不保留蜂窝以及TC仓索引请将drop置为True
            df_tc = df_tc[[by_name[0], by_name[1], by_tc, '指标', "{}日".format(timeday[2])]]
            transpose_df_list.append(df_tc)
    df = pd.concat(transpose_df_list)  # 数据拼接
    return df


def replace_var(iter_name: Iterable):
    global df_cal  # 每次调用将修改 全局变量df_cal的值
    df_list = []
    for group_name, df in df_cal.groupby(by=iter_name):
        if df.shape[0] > 1:
            # 其余值替换为0(此处将[第一行迭代字段的值保留])
            df.loc[df.index[1:].tolist(), iter_name[1]] = 0
        df_list.append(df)
    if df_list:  # 成功返回1
        df_cal = pd.concat(df_list)
        return 1
    else:
        print(" %s当前分组内的字段存在异常导致数据为空" % iter_name[1])
        return 0  # 失败返回0

        # 福州看板


def fuzhou(df: pd.core.frame.DataFrame):
    tz_id = '团长ID'
    date_index = (datetime.datetime.now() -
                  datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # 明确返回副本,而不是切片,否则以下赋值 可能会导致pandas不知道是在原有对象
    df_copy = df[df['下单日期'] == '2021-07-02'].copy()
    # 进行修改 还是副本修改

    '''
    对已知创建副本,
    如果您希望 Pandas 或多或少地信任链式索引表达式的赋值，
    您可以将该选项 设置mode.chained_assignment为以下值之一：
    pd.set_option('mode.chained_assignment','warn')
    '''
    df_copy['业务类型'] = df_copy['业务类型'].str.replace(
        r'(^淘外团购$)|(^淘内团购$)', '团购业务')  # 查看原有对象,如果没有调用copy函数 df与df_copy会产生与预期不一样的结果
    df_copy["件数"] = df_copy['销售件数'] - df_copy['撤单件数']  # 当日实际件数
    # 三目运算符 如果件数列 迭代元素，元素值 > 1返回1,否则返回0
    df_copy['是否动销团长'] = np.where(df_copy['件数'] >= 1, 1, 0)
    df_copy = pd.merge(df_copy, df_tz_base, left_on=tz_id,
                       right_on=tz_id, how='left')
    # 将df_cal视为全局变量
    global df_cal

    df_cal = df_copy.loc[:, ['城市圈', '作战部', '战区', '主站', '子站', 'TC仓', '蜂窝', tz_id, 'gmv',
                             'nmv', '高质量nmv', '销售件数', '业务类型', '撤单件数', '拉新人数', '团长当日下单用户数', '团长uv', '件数', '是否动销团长']]
    # 迭代分组对象 列表中 字段列可能存在缺失  请考虑是否填充
    group_list = [['团长ID', i] for i in ['团长当日下单用户数', '团长uv', '是否动销团长']]
    flag = list(map(replace_var, group_list))  # flag标记是否执行成功  输出看板数据

    df_cal.to_excel('E:/钉钉excel文件/result/{}月看板源数据{}.{}.xlsx'.format(
        timeday[1], timeday[1], timeday[2]), index=False)  # 结果输出到文件

    # 转置操作
    df_cal['件达标团长数'] = df_cal['件数'].apply(lambda x: 1 if x >= 15 else 0)
    df_cal_sum = df_cal.groupby(by=tz_id).sum()
    df_tz = pd.merge(df_cal_sum, df_cal.loc[:, [tz_id, '城市圈', '蜂窝', 'TC仓']].drop_duplicates(
        subset=[tz_id]), left_on=tz_id, right_on=tz_id, how='left').drop(columns=['销售件数', '撤单件数']).rename(
        columns={'gmv': 'GMV', 'nmv': 'NMV', '高质量nmv': '高质量NMV', '是否动销团长': "动销团长数",
                 '团长uv': 'UV访问量', '团长当日下单用户数': "下单用户数", '拉新人数': '新用户数'})  # 星级团长

    df_tz_sum = df_tz.groupby('TC仓').sum()  # 此处可能会产生 inf值 对于除数为0的情况暂时不做处理
    df_tz_sum['人均购买件数'] = df_tz_sum['件数'] / df_tz_sum['下单用户数']
    df_tz_sum['单团用户数'] = df_tz_sum['下单用户数'] / df_tz_sum['动销团长数']
    df_tz_sum['团效-件数'] = df_tz_sum['件数'] / df_tz_sum['动销团长数']
    df_tz_cal = pd.merge(df_tz_sum,
                         df_tz.drop_duplicates(
                             subset=['TC仓']).loc[:, ['城市圈', 'TC仓', '蜂窝']],
                         left_on='TC仓', right_on='TC仓', how='right')
    df_tz_cal = df_tz_cal.loc[:, ['城市圈', '蜂窝', 'TC仓', 'GMV', 'NMV', '高质量NMV', '动销团长数',
                                  '件数', 'UV访问量', '下单用户数', '新用户数', '人均购买件数', '单团用户数', '团效-件数', '件达标团长数']]
    # 调用转置函数
    df_result = transpose_df(df_tz_cal, ["城市圈", "蜂窝"])  # 不需要在原函数修改索引 保存到文件时置index为False
    df_result.to_excel('E:/钉钉excel文件/result/福州蜂窝-TC看板{}.{}.xlsx'.format(
        timeday[1], timeday[2]), index=False)  # 结果输出到桌面


# 计算达标团
def chief_pass(*df_tuple: Tuple[pd.core.frame.DataFrame]):
    day, tz_id = 7, '团长ID'
    # chief_sale 团长id无需去重, tz_information, tz_sale[['下单日期', '首单日期', tz_id, '团长佣金']]
    df_chief_sale, df, tz_sale = df_tuple  # 解包
    import datetime as dt
    df['首单结束日期'] = df['首单日期'] + dt.timedelta(days=day - 1)
    df_chief_sale['首单日期差'] = (
        df_chief_sale['下单日期'] - df_chief_sale['首单日期']).dt.days

    # 该表为团长销售日报的数据切片 每月推前days天(函数chief_date中的chief_sale返回)
    df_chief_sale = df_chief_sale[[tz_id, 'nmv',
                                   '拉新人数', '销售件数']][df_chief_sale['首单日期差'] < day]
    df_chief_sale = df_chief_sale.groupby(by=[tz_id]).sum()
    df_chief_sale = df_chief_sale.rename(
        columns={'nmv': '{}天nmv'.format(day), '拉新人数': '{}天拉新人数'.format(day), '销售件数': '{}天销售件数'.format(day)})  # 修改索引名称
    #  判断days天是否达标 连接cms_tz表  #保存df对象中的子站,去除cms_tz中的子站

    df = pd.merge(df, df_cms_tz.drop(columns=["子站"]), left_on=tz_id,
                  right_on=tz_id, how='left')

    df = pd.merge(df, df_chief_sale, left_on=tz_id,
                  right_on=tz_id, how='left')

    df['{}天达标'.format(day)] = df.apply(lambda x: '达标' if x['{}天nmv'.format(day)] >= 300 and x['{}天拉新人数'.format(day)]
                                                                                            >= 5 and x['{}天销售件数'.format(day)] >= 30 else '不达标', axis=1)
    # 计算合格团
    tz_sale['首单日期差'] = tz_sale['首单日期差'] = (
        tz_sale['下单日期'] - tz_sale['首单日期']).dt.days
    for day in [7, 30]:
        days_data = tz_sale[tz_sale['首单日期差'] < day]
        day_name = "{}天团长佣金".format(day)
        days_data = days_data.rename(columns={'团长佣金': day_name})
        days_sum = days_data[day_name].groupby(days_data['团长ID']).sum()
        df = pd.merge(left=df, right=days_sum, left_on=tz_id,
                      right_on=tz_id, how='left')

    df['达标团'] = df.apply(lambda x: '合格' if x['7天团长佣金'] >=
                                           20 or x['30天团长佣金'] >= 200 else '不合格', axis=1)
    df = df.replace('#N/A', np.nan)
    df = df.drop(columns=['手机号', '主站', '团长收益倍数',
                          '团长信用评级', '送货上门', '状态', '营业状态', '创建时间', '时间', '运营人'])
    df.to_excel('E:/钉钉excel文件/result/{}月达标合格团{}.{}.xlsx'.format(
        timeday[1], timeday[1], timeday[2]), index=False)


def chief_date(df: pd.core.frame.DataFrame):
    date_index = datetime.datetime.now()
    if date_index.strftime('%d') == "01":  # 当前时间为当月的1号
        date_index = date_index - datetime.timedelta(days=1)  # 定位到上月的最后一天
    date_index = date_index.strftime("%Y-%m-%d").split("-")  # 上月一号时间列表
    date_index[2] = "01"
    date_index = "-".join(date_index)

    date_index = ((datetime.datetime.strptime(date_index, '%Y-%m-%d')) -
                  datetime.timedelta(days=8)).strftime('%Y-%m-%d')  # 往前推9天
    tz_sale = df[(
                     df['下单日期'] >= date_index) & (df['首单日期'] >= date_index)]

    # 团长销售信息
    tz_id = '团长ID'
    chief_sale = tz_sale[['下单日期', '首单日期', tz_id, 'nmv', '下单用户数', '销售件数']]
    # 团长基本信息 主站 此处团长ID(chief_message变量有重复)需要去重
    chief_message = tz_sale[[tz_id, '首单日期']].drop_duplicates(tz_id)

    # 连接tz_base表 ,获取团长更详细的信息
    tz_information = pd.merge(
        chief_message, df_tz_base.set_index([tz_id]), left_on=tz_id, right_on=tz_id, how='left')

    tz_information = tz_information[[
        '蜂窝城市圈', '蜂窝', 'TC仓', tz_id, '首单日期']]  # 蜂窝和TC仓加入到该表中
    # 将合格团所需数据返回
    return chief_sale, tz_information, tz_sale[['下单日期', '首单日期', tz_id, '团长佣金']]


def df_sum(*df_tuple: Tuple[pd.core.frame.DataFrame]):
    df_sale, df_cms, df_base, df_tz_month_sale = df_tuple
    tz_id = "团长ID"

    date_index = datetime.datetime.now()  # 获取当前时间
    # 当前时间为当月的date_index - datetime.timedelta(days=1)  # 定位到上月的最后一天
    if date_index.strftime('%d') == "01":
        date_index = date_index - datetime.timedelta(days=1)
    date_index = date_index.strftime("%Y-%m-%d").split("-")  # 上月一号时间列表
    date_index[2] = "01"
    date_index = "-".join(date_index)

    df_month_sale = df_tz_month_sale[df_tz_month_sale['订单月份'] >= date_index].copy(
    )
    df_month_sale['生鲜件数占比'] = df_month_sale['月累计生鲜件数'] / \
                              df_month_sale['月累计件数']
    df_month_sale['星级'] = df_month_sale.apply(get_star, axis=1)
    df_month_sale = df_month_sale.drop(columns=['nmv'])

    # 切片
    df_sale = df_sale[df_sale['下单日期'] >= date_index]
    # 该表作为基础表连接其他表 月聚合
    df_sale_duplicates = df_sale.loc[:, [tz_id]
                         ].drop_duplicates(tz_id)  # 取出需要计算的字段，去重

    # bd数据 重命名列名方便 连接操作 BD推荐人 邀请人ID在tz_base表中也能获得 df_cms表中 团长ID是主键不需要去重
    df_cms = df_cms.loc[:, [tz_id, 'BD推荐人', '邀请人ID']]

    for df in [df_month_sale, df_cms, df_base]:
        df_sale_duplicates = pd.merge(
            df_sale_duplicates, df, left_on=tz_id, right_on=tz_id, how='left')
    # 取出动销团数据 团长聚合
    df_moving_sale = df_sale.drop(['作战部', '战区', '城市圈', '主站', '子站', '业务类型'],
                                  axis=1)  # 取出动销数据，删除不要的字段
    df_moving_sale_sum = df_moving_sale.groupby(by=tz_id).sum()

    df_moving_sale_sum.rename(columns={'下单用户数': '下单次数'}, inplace=True)
    df_moving_sale_duplicates = pd.merge(
        df_moving_sale_sum, df_sale_duplicates, left_on=tz_id, right_on=tz_id, how='left')  # 左连接团长其他数据

    df_moving_sale_duplicates = df_moving_sale_duplicates.loc[:, ['子站', '蜂窝城市圈', '蜂窝', 'TC仓', tz_id, '首单日期', '星级', 'BD推荐人',
                                                                  '邀请人ID', '高质量nmv', 'gmv', 'nmv', '团长自购nmv', '销售件数', '月下单用户数', '下单次数', '拉新人数']]

    df_moving_sale_duplicates.to_excel(
        'E:/钉钉excel文件/result/{0}月月动销{1}.{2}.xlsx'.format(timeday[1], timeday[1], timeday[2]), index=False)


if __name__ == "__main__":
    source_data()
    #     # 计算达标团
    chief_sale, tz_information, tz_sale = chief_date(
        df_tz_day_sale.loc[:, ["城市圈", "战区", "主站", "团长ID", "首单日期", "下单日期", "nmv", "下单用户数", "销售件数", "团长佣金"]])

