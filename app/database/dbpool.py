# -*- coding:utf-8 -*-
# ===============================================
# 文件描述：
# 作   者：HUP
# 日   期: 2025/3/25
# 邮   箱: 28971609@qq.com
# ===============================================

import sys
import pyodbc
import pymysql
import logging
from dbutils.pooled_db import PooledDB


class DBConnectedPool:
    pool = None

    def __init__(self, conn_params={},
                 mincached=1,  # 连接池里面初始的空闲连接（默认0）
                 maxcached=1,  # 最大的空闲连接数，如果空闲连接数大于当前值，pool会关闭空闲连接 ，默认0 = 无限制
                 maxshared=0,  # 最大的允许共享的连接数，默认0 = 都不共享
                 maxconnections=1,  # 最大的连接数
                 maxusage=None,  # 单个连接最大可以被复用的次数
                 blocking=True,  # 当连接数达到最大连接数时，true=新连接会一直等待
                 ping=7,  # Ping:确定何时应该使用Ping()检查连接(0 = None = never, 1 = default =无论何时从池中取出，
                 # 2 =创建游标时，4 =执行查询时，7 = always，以及这些值的所有其他位组合)
                 # reset=True       # 当连接放回连接池时，是否每次都调用rollback,以保证事务终止，为False 或 None 时不额外调用
                 ):
        try:
            match conn_params.get("dbms").lower():
                case 'sqlserver':
                    self.pool = PooledDB(
                        creator=pyodbc.connect,
                        mincached=mincached,
                        maxcached=maxcached,
                        maxshared=maxshared,
                        maxconnections=maxconnections,
                        blocking=blocking,
                        maxusage=maxusage,
                        ping=ping,
                        driver="{SQL Server}" if "win" in sys.platform else "{FreeTDS}",
                        server=conn_params['host'],
                        port=conn_params['port'],
                        user=conn_params['user'],
                        password=conn_params['password'],
                        database=conn_params['database'],
                        autocommit=True,
                        timeout=60
                    )
                case "mysql":
                    self.pool = PooledDB(
                        creator=pymysql,
                        mincached=1,
                        maxcached=2,
                        maxconnections=8,
                        blocking=True,
                        ping=7,
                        host=conn_params['host'],
                        port=int(conn_params['port']),
                        user=conn_params['user'],
                        password=conn_params['password'],
                        database=conn_params['database'],
                        autocommit=True,
                        connect_timeout=60,
                    )
                case _:
                    logging.error("数据库类型[dbms]定义错误, 请检查")
        except Exception as ex:
            logging.error(f"连接数据库{conn_params['host']},{conn_params['port']}失败 错误：{ex}")

    def get_conn(self):
        return self.pool.connection() if self.pool else False
