# -*- coding:utf-8 -*-
# ===============================================
# 文件描述：
# 作   者：HUP
# 日   期: 2025/3/25
# 邮   箱: 28971609@qq.com
# ===============================================
import pyodbc
import logging


def execute(conn, sql) -> dict:
    """
    功能: 执行sql 并返回状态
    :return: dict
        Code: int ProgrammingError=-2  非致命错误, 一般指sql语句中的错误, 调用者可根据情况决定是否续续.DatabaseError =-1  致命错误, 调用者应终止程序的执行.
        Msg:  str 描述信息
    """
    rs = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            is_next = True
            while is_next:
                if cur.description:
                    # columns = [column[0] for column in cur.description]
                    cols = [str(column[0]).capitalize() for column in cur.description]

                    if "Code" in cols and "Msg" in cols:
                        row = cur.fetchone()
                        rs = dict(zip(cols, row))
                is_next = cur.nextset()
    except pyodbc.ProgrammingError as ex:
        match "Previous SQL was not a query" in str(ex):
            case True:
                rs = {"Code": 1, "Msg": f"{ex}"}
            case _:
                rs = {"Code": -2, "Msg": f"{ex}"}
                logging.error(ex)
        return rs
    except Exception as ex:
        rs = {"Code": -1, "Msg": f"{ex}"}
        logging.error(ex)
        return rs

    if not ("Code" in str(rs.keys()).title() and "Msg" in str(rs.keys()).title()):
        rs = {"Code": 1, "Msg": "SQL中未返回状态说明"}

    return rs


def query(conn, sql) -> tuple[bool, list]:
    """
    功能：执行sql 返回数据
    :param conn:
    :param sql:
    :return:”Code": -2 非致命错误, 一般指sql语句中的错误, 调用者可根据情况决定是否续续. DatabaseError ：-1  致命错误, 调用者应终止程序的执行.
            "Msg":str -> 返回的说明
    """
    rd = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                columns = [column[0] for column in cur.description]
                data = cur.fetchall()
                for d in data:
                    rd.append(dict(zip(columns, d)))
    except Exception as ex:
        logging.error(f"{ex}")
        return False, rd

    return True, rd


def call(conn, sql):
    """
    功能：执行存储过程 返回数据和状态
    :param conn:
    :param sql:
    :return: rd:lsit ,rs:dict
    ”Code": int  -> ProgrammingError ：-2  非致命错误, 一般指sql语句中的错误, 调用者可根据情况决定是否续续.
            DatabaseError ：-1  致命错误, 调用者应终止程序的执行.
    "Msg":str    -> 返回的说明
    """
    rd = []
    rs = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            is_next = True
            while is_next:
                if cur.description:
                    rst = cur.fetchall()
                    columns = [column[0] for column in cur.description]
                    cols = [str(column[0]).capitalize() for column in cur.description]
                    if "Code" in cols and "Msg" in cols:
                        state = rst[0]
                        rs = dict(zip(cols, state))
                    else:
                        for d in rst:
                            rd.append(dict(zip(columns, d)))
                is_next = cur.nextset()
    except Exception as ex:
        rs = {"Code": -1, "Msg": f"{ex}"}
        logging.error("{}".format(ex))
        return rd, rs

    if not ("Code" in rs.keys() and "Msg" in rs.keys()):
        rs = {"Code": 1, "Msg": "SQL中未返回状态说明"}

    return rd, rs
