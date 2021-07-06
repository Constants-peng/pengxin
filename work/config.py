import os
import yaml
import redis

yamlpath = os.path.join(os.getcwd(), "config.yaml")
with open(yamlpath, 'r', encoding='utf-8') as f:
    cfg = f.read()
cfg = yaml.load(cfg, Loader=yaml.FullLoader)


def get_redis_pool():
    pool = redis.ConnectionPool(**cfg['redis'])
    redis_pool = redis.StrictRedis(connection_pool=pool)
    return redis_pool


def get_mysql_cfg(mysql_cfg=None):
    if mysql_cfg is None or mysql_cfg == "test":
        return cfg['test_mysql_cfg']
    elif mysql_cfg == "publish":
        return cfg['public_mysql_cfg']
    else:
        raise Exception("{}配置连接对mysql数据库有误".format(mysql_cfg))


def get_table_cfg(table_source: str):
    return cfg[table_source]


if __name__ == "__main__":
    for table_name, fields in get_table_cfg().items():
        print(table_name, fields)
