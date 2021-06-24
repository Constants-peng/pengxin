import pandas as pd
import os
file_list = []
# 将目录下的文件名添加到file_list列表中
def file_name_walk(file_dir):
    for root, dirs, files in os.walk(file_dir):
        for file_name in files:
            file_list.append(os.path.join(root,file_name))
path = r'E:\钉钉excel文件\624团长-商品'     #文件目录
file_name_walk(path)
df_list = [pd.DataFrame() for i in range(len(file_list))]
df_dict = dict(zip(file_list,df_list))
for file_name in df_dict.keys():
    df_dict[file_name] = pd.read_excel(file_name)
df = pd.concat(df_dict.values())
df.to_excel(path+'\测试数据.xlsx',index=False)     #保存文件路径