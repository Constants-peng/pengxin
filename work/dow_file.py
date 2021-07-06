import requests
import pandas as pd
import ipynb_importer
from config import get_redis_pool
import threading
import re

df = pd.read_excel('E:\钉钉excel文件\竞对.xlsx')
# df = df[df['花心'] == '福州1']
# df['战区'] = ["粤东战区" for i in range(df.shape[0])]


phone_list, order_list, redis_pool = [], [], get_redis_pool()
flag_url = redis_pool.llen("url")
# 定义函数用来获取http响应界面


def get_image(url):
    response = requests.get(url=url, headers={
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'})
    if response.status_code == 200:
        return response.content
    else:
        return False


def join_str(x):  # 对dataframe元素按行进行操作
#     x_phone = x['战区']+"-"+x['BD姓名']+'-电话-'+str(x['提报团长手机号码'])  # 电话文件名
#     x_order = x['战区']+"-"+x['BD姓名']+'-订单-'+str(x['提报团长手机号码'])  # 订单文件名
    x_phone = x['BD姓名']+'-电话-'+str(x['提报团长手机号码'])  # 电话文件名
    x_order = x['BD姓名']+'-订单-'+str(x['提报团长手机号码'])  # 订单文件名
    phone_list.append(x_phone), order_list.append(x_order)
    # 判断flag_phone状态 如果大于0不进行lpush操作,否则进行lpush
    if flag_url == 0:
        redis_pool.lpush("url", x_phone+":" +
                         x['竞对团长后台显示手机号码截图'], x_order + ":"+x['竞对团长交易额≥3000截图'])


df.apply(join_str, axis=1)  # 对每一行的元素进行操作
df['电话号码'], df['订单号码'] = phone_list, order_list


def download_image():
    while True:
        url_list = redis_pool.rpop("url")
        if url_list:
            url_list = "".join(url_list)
            url = re.match(r'^(.*?):(http.*?\.jpeg).*?',
                           url_list, re.S)
            response = get_image(url.group(2))
            if response is False:
                print("解析错误:", url)   #解析错误url重新加入到redis中 调试
                redis_pool.lpush("url",url)
            else:
                with open(r'E:\钉钉excel文件\image_down\{0}.jpg'.format(url.group(1)), 'wb') as f:
                    f.write(response)
        else:
            break


thread_list = [threading.Thread(target=download_image) for i in range(10)]
[i.start() for i in thread_list]
[i.join() for i in thread_list]