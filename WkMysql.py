# -*- coding: utf-8 -*-
# @Date     : 2023-10-13 13:00:00
# @Author   : WangKang
# @Blog     :
# @Email    : 1686617586@qq.com
# @Filepath : WkMysql.py
# @Brief    : 封装数据库操作，适合单线程使用，且短时间连接数据库
# Copyright 2023 WANGKANG, All Rights Reserved.

""" 
项目地址：https://gitee.com/purify_wang/wkmysql
"""

import pymysql
from pymysql import cursors
import sys
from WkLog import log

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "test_table"


class DB:
    def __init__(
        self,
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        port=PORT,
        cursorclass=cursors.DictCursor,
        **kwargs,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.cursorclass = cursorclass
        self.kwargs = kwargs

        self.table = None
        self.conn = self.connect_db()

    def connect_db(self) -> pymysql.Connection:
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                port=self.port,
                passwd=self.password,
                database=self.database,
                autocommit=True,
                cursorclass=self.cursorclass,
                **self.kwargs,
            )
            log.debug("Successfully connected to database!")
            return conn
        except Exception as e:
            msg = f"Failed to connect to database! -> {str(e)}"
            log.error(msg)
            raise Exception(msg)

    def close(self):
        self.conn.close()

    def before_execute(func):
        def wrapper(self, *args, **kwargs):
            self.__test_conn()
            if self.table is None:
                raise Exception("table is not set!")
            result = func(self, *args, **kwargs)
            return result

        return wrapper

    def __test_conn(self):
        """
        长连接时，如果长时间不进行数据库交互，连接就会关闭，再次请求就会报错
        每次使用游标的时候，都调用下这个方法
        """
        # log.debug("__test_conn")
        # self.conn.ping()
        try:
            self.conn.ping()
        except:
            self.conn = self.connect_db()

    def __get_query_params(self, obj: dict | list):
        if isinstance(obj, dict):
            return " AND ".join([f"`{column_name}` {'=' if obj[column_name] is not None else 'is'} %s" for column_name in obj.keys()])
        elif isinstance(obj, list):
            return " AND ".join([f"`{column_name}` {'=' if column_name is not None else 'is'} %s" for column_name in obj])

    def __get_set_params(self, obj: dict):
        return ", ".join([f"`{column_name}` {'=' if obj[column_name] is not None else 'is'} %s" for column_name in obj.keys()])

    def __get_col_params(self, obj: dict | list):
        if isinstance(obj, dict):
            return ", ".join([f"`{column_name}`" for column_name in obj.keys()])
        elif isinstance(obj, list):
            return ", ".join([f"`{column_name}`" for column_name in obj])

    def __get_values(self, obj: dict | list):
        if isinstance(obj, dict):
            return list(obj.values())
        elif isinstance(obj, list):
            res = []
            for o in obj:
                res.append(self.__get_values(o))
            return res

    def __get_placeholders(self, length):
        return ", ".join(["%s"] * length)

    def __validate_args(self, args, kwargs):
        """
        验证参数是否正确
        """
        if not args and not kwargs:
            raise Exception("args or kwargs must be used!")
        if args and kwargs:
            raise Exception("args and kwargs cannot be used together!")
        if args:
            if len(args) > 1 or not isinstance(args[0], dict):
                raise Exception("args's length must be 1 and the type must be dict!")

    def __print_info(self, cursor, func_name, success=True, error_msg=None):
        if success:
            log.debug(f"Success: {func_name} -> {cursor._executed if type(cursor._executed) == str else cursor._executed.decode()} -> Rows affected: {cursor.rowcount}")
        else:
            log.error(f"Failure: {func_name} -> {f'{cursor._executed if type(cursor._executed) == str else cursor._executed.decode()}' if cursor._executed else 'None'} -> {error_msg}")

    def set_table(self, table):
        self.table = table
        return self

    @before_execute
    def create_table(self, obj: dict, delete_if_exists=False):
        """
        创建表
        :param obj: 字典对象，键为列名，值为列类型
        :param delete_if_exists: 是否删除原有表
        :return: True/False
        """
        col_params = ", ".join([f"`{column_name}` {column_type}" for column_name, column_type in obj.items()])
        if delete_if_exists:
            self.delete_table()

        sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({col_params})"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False

    @before_execute
    def delete_table(self):
        """
        删除表
        :return: True/False
        """
        sql = f"DROP TABLE IF EXISTS {self.table}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False

    def get_column_names(self):
        sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, (self.database, self.table))
                res = []
                for row in cursor.fetchall():
                    res.append(row.get("COLUMN_NAME"))
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return res
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return []

    @before_execute
    def exists(self, *args, **kwargs):
        """
        根据字典对象判断元素是否存在
        - demo:
            - exists({"id": 1, "name": "wangkang"})
            - exists({id=1, name=wangkang})
        """
        self.__validate_args(args, kwargs)
        obj = args[0] if args else kwargs

        values = self.__get_values(obj)
        params = self.__get_query_params(obj)
        sql = f"SELECT 1 FROM {self.table} WHERE {params} LIMIT 1"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
                flag = cursor.fetchone() != None
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return flag
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False

    @before_execute
    def insert_row(self, *args, **kwargs):
        """
        插入一行数据
        :return: True/False, insert_id
        - demo:
            - insert_row({"id": 1, "name": "wangkang"})
            - insert_row(id=1, name=wangkang)
        """
        self.__validate_args(args, kwargs)
        obj = args[0] if args else kwargs

        values = self.__get_values(obj)
        col_params = self.__get_col_params(obj)
        placeholders = self.__get_placeholders(len(obj))
        sql = f"INSERT INTO {self.table}({col_params}) VALUES({placeholders})"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
                self.conn.commit()
                insert_id = cursor.lastrowid  # 获取插入数据的自增ID,如果没有自增ID，则返回0
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True, insert_id
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False, -1

    def insert_rows(self, obj_list: list[dict]):
        """
        插入多行数据，不会因为个别数据的添加失败导致后面的所有数据都插入失败
        :param obj_list: 列表，元素为字典对象，键为列名，值为列值
        :return: 字典对象，键为success和fail，值为成功和失败次数
        """
        if not obj_list:
            return None
        success = 0
        fail = 0
        for obj in obj_list:
            flag, _ = self.insert_row(obj)
            if flag:
                success += 1
            else:
                fail += 1
        return {"success": success, "fail": fail}

    @before_execute
    def insert_many(self, obj_list: list[dict]):
        """
        使用executemany来批量插入数据 此操作具有原子性
        obj_list: 列表，元素为字典对象，键为列名，值为列值
        :return: True/False
        """
        if not obj_list:
            log.warn("要插入的数据为空!")
            return
        values = self.__get_values(obj_list)
        col_params = self.__get_col_params(obj_list[0])
        placeholders = self.__get_placeholders(len(obj_list[0].keys()))
        sql = f"INSERT INTO {self.table}({col_params}) VALUES({placeholders})"
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(sql, values)
                self.conn.commit()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True
        except Exception as e:
            self.conn.rollback()
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False

    @before_execute
    def delete_row(self, *args, **kwargs):
        """
        根据条件删除一行数据
        - demo:
            - delete_row({"id": 1, "name": "wangkang"})
            - delete_row(id=1, name=wangkang)
        """
        self.__validate_args(args, kwargs)
        obj = args[0] if args else kwargs

        values = self.__get_values(obj)
        params = self.__get_query_params(obj)
        sql = f"DELETE FROM {self.table} WHERE {params}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
                self.conn.commit()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False

    def delete_rows(self, obj_list: list):
        """
        :param obj_list: 列表，元素为字典对象，键为列名，值为列值
        :return: 字典对象，键为success和fail，值为成功和失败次数
        """
        if not obj_list:
            return None
        success = 0
        fail = 0
        for obj in obj_list:
            if self.delete_row(obj):
                success += 1
            else:
                fail += 1
        return {"success": success, "fail": fail}

    @before_execute
    def delete_many(self, obj_list: list[dict]):
        """
        使用executemany来批量插入数据 此操作具有原子性
        :param obj_list: 列表，元素为字典对象，键为列名，值为列值
        :return: True/False
        """
        if not obj_list:
            log.warn("要删除的数据为空!")
            return
        values = self.__get_values(obj_list)
        params = self.__get_query_params(obj_list[0])
        print(params)
        sql = f"DELETE FROM {self.table} WHERE {params}"
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(sql, values)
                self.conn.commit()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return False

    @before_execute
    def select_all(self):
        """
        查询所有数据
        :return: 列表，元素为字典对象，键为列名，值为列值
        """
        sql = f"SELECT * FROM {self.table}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return data
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return None

    @before_execute
    def select(self, *args, **kwargs):
        """
        根据条件进行查询
        :return: 列表，元素为字典对象，键为列名，值为列值
        - demo:
            - select({"id": 1, "name": "wangkang"})
            - select(id=1, name=wangkang)
        """
        self.__validate_args(args, kwargs)
        obj = args[0] if args else kwargs

        values = self.__get_values(obj)
        param = self.__get_query_params(obj)
        # print(f"|{col_names}|{col_params}|{values}|{param}|")
        sql = f"SELECT * FROM {self.table} where {param}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
                data = cursor.fetchall()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return data
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return None

    @before_execute
    def select_one(self, *args, **kwargs):
        """
        根据条件进行查询，只返回第一条数据
        :return: 字典对象，键为列名，值为列值
        - demo:
            - select_one({"id": 1, "name": "wangkang"})
            - select_one(id=1, name=wangkang)
        """
        self.__validate_args(args, kwargs)
        obj = args[0] if args else kwargs

        values = self.__get_values(obj)
        param = self.__get_query_params(obj)
        sql = f"SELECT * FROM {self.table} where {param} LIMIT 1"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
                data = cursor.fetchone()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return data
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            return None

    @before_execute
    def update(self, target_obj: dict, new_obj: dict):
        """
        根据条件更新数据
        :param target_obj: 字典对象，键为列名，值为列值 作为更新条件
        :param new_obj: 字典对象，键为列名，值为列值 作为更新内容
        :return: True/False
        """
        values = self.__get_values(new_obj) + self.__get_values(target_obj)
        set_params = self.__get_set_params(new_obj)
        query_params = self.__get_query_params(target_obj)
        sql = f"UPDATE {self.table} set {set_params} where {query_params}"
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, values)
                self.conn.commit()
                self.__print_info(cursor, sys._getframe().f_code.co_name)
            return True
        except pymysql.Error as e:
            self.__print_info(cursor, sys._getframe().f_code.co_name, success=False, error_msg=str(e))
            self.conn.rollback()
            return False
