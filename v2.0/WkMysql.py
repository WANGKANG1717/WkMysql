# -*- coding: utf-8 -*-
# @Date     : 2023-10-13 13:00:00
# @Author   : WangKang
# @Blog     : kang17.xyz
# @Email    : 1686617586@qq.com
# @Filepath : WkMysql.py
# @Brief    : 封装数据库操作
# Copyright 2023 WANGKANG, All Rights Reserved.

from tkinter import NO
from numpy import isin
import pymysql
from sympy import EX
from WkLog_new import log

HOST = "localhost"
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "authentication"


class DB:
    def __init__(
        self, host=HOST, user=USER, password=PASSWORD, database=DATABASE, table=TABLE
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.table = table

        self.db = None
        self.cursor = None

        self.connect_db()

    def connect_db(self):
        try:
            self.db = pymysql.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
                database=self.database,
                autocommit=True,
            )
            self.cursor = self.db.cursor()
            log.info("连接数据库成功")
        except Exception as e:
            msg = f"连接数据库失败 -> {str(e)}"
            log.error(msg)
            raise Exception(msg)

    # 长连接时，如果长时间不进行数据库交互，连接就会关闭，再次请求就会报错
    # 每次使用游标的时候，都调用下这个方法
    def test_conn(self):
        # 这两种方法本质上是一致的
        self.db.ping(reconnect=True)
        # try:
        #     self.db.ping()
        # except:
        #     self.connect_db()

    def get_query_params(self, obj: dict | list):
        if isinstance(obj, dict):
            return " AND ".join(
                [
                    f"`{column_name}` {'=' if obj[column_name] is not None else 'is'} %s"
                    for column_name in obj.keys()
                ]
            )
        elif isinstance(obj, list):
            return " AND ".join(
                [
                    f"`{column_name}` {'=' if column_name is not None else 'is'} %s"
                    for column_name in obj
                ]
            )

    def get_set_params(self, obj: dict):
        return ", ".join(
            [
                f"`{column_name}` {'=' if obj[column_name] is not None else 'is'} %s"
                for column_name in obj.keys()
            ]
        )

    def get_col_params(self, obj: dict | list):
        if isinstance(obj, dict):
            return ", ".join([f"`{column_name}`" for column_name in obj.keys()])
        elif isinstance(obj, list):
            return ", ".join([f"`{column_name}`" for column_name in obj])

    def get_values(self, obj: dict | list):
        if isinstance(obj, dict):
            return list(obj.values())
        elif isinstance(obj, list):
            res = []
            for o in obj:
                res.append(self.get_values(o))
            return res

    def get_placeholders(self, length):
        return ", ".join(["%s"] * length)

    def package_data(self, data_list: list | tuple, col_names: list | tuple):
        """用来封装数据: 把数据封装为json数据"""
        json_data = []
        try:
            for data in data_list:
                tmp_dict = {}
                for idx, value in enumerate(data):
                    tmp_dict[col_names[idx]] = value
                json_data.append(tmp_dict)
            return json_data
        except Exception as e:
            log.error(f"封装数据失败 -> {str(e)}")
            return None

    # 根据关键词判断某一列是否存在
    def exists(self, column_name, value):
        return self.exists_by_obj({column_name: value})

    # 根据字典对象判断元素是否存在
    def exists_by_obj(self, obj: dict):
        self.test_conn()
        values = self.get_values(obj)
        params = self.get_query_params(obj)
        sql = f"SELECT 1 FROM {self.table} WHERE {params} LIMIT 1"
        try:
            self.cursor.execute(sql, values)
            flag = self.cursor.fetchone() != None
            log.debug(
                f"查询成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return flag
        except pymysql.Error as e:
            log.error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 插入单行数据
    def insert_row(self, obj: dict):
        self.test_conn()
        # print("insert_row")
        values = self.get_values(obj)
        col_params = self.get_col_params(obj)
        placeholders = self.get_placeholders(len(obj))
        sql = f"INSERT INTO {self.table}({col_params}) VALUES({placeholders})"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            log.debug(
                f"插入成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return True
        except pymysql.Error as e:
            log.error(f"插入失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 插入多行数据
    # 不会因为个别数据的添加失败导致后面的所有数据都插入失败
    # 返回成功次数和失败次数
    def insert_rows(self, obj_list: list[dict]):
        if not obj_list:
            return None
        success = 0
        fail = 0
        for obj in obj_list:
            if self.insert_row(obj):
                success += 1
            else:
                fail += 1
        return {"success": success, "fail": fail}

    def insert_many(self, col_names: list, obj_list: list[dict]):
        """使用executemany来批量插入数据 此操作具有原子性"""
        if not obj_list:
            return
        self.test_conn()
        values = self.get_values(obj_list)
        col_params = self.get_col_params(col_names)
        placeholders = self.get_placeholders(len(col_names))
        sql = f"INSERT INTO {self.table}({col_params}) VALUES({placeholders})"
        try:
            self.cursor.executemany(sql, values)
            self.db.commit()
            log.debug(
                f"插入成功 -> {self.cursor._last_executed.decode()} -> 影响行数: {self.cursor.rowcount}"
            )
            return True
        except pymysql.Error as e:
            self.db.rollback()
            log.error(f"插入失败 -> {self.cursor._last_executed.decode()} -> {str(e)}")
            return False

    # 列名和值
    def delete_row(self, column_name, value):
        return self.delete_row_by_obj({column_name: value})

    # 字典对象
    def delete_row_by_obj(self, obj: dict):
        self.test_conn()
        values = self.get_values(obj)
        params = self.get_query_params(obj)
        sql = f"DELETE FROM {self.table} WHERE {params}"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            log.debug(
                f"删除成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return True
        except pymysql.Error as e:
            log.error(f"删除失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    def delete_rows_by_obj(self, obj_list: list):
        if not obj_list:
            return None
        success = 0
        fail = 0
        for obj in obj_list:
            if self.delete_row_by_obj(obj):
                success += 1
            else:
                fail += 1
        return {"success": success, "fail": fail}

    def delete_many_by_obj(self, col_names: list, obj_list: list[dict]):
        """使用executemany来批量插入数据 此操作具有原子性"""
        if not obj_list:
            return
        self.test_conn()
        values = self.get_values(obj_list)
        print(values)
        params = self.get_query_params(col_names)
        print(params)
        sql = f"DELETE FROM {self.table} WHERE {params}"
        try:
            self.cursor.executemany(sql, values)
            self.db.commit()
            log.debug(
                f"删除成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return True
        except pymysql.Error as e:
            log.error(f"删除失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 查询所有数据
    def select_all(self):
        self.test_conn()
        col_names = self.get_column_names()
        col_params = self.get_col_params(col_names)
        sql = f"SELECT {col_params} FROM {self.table}"
        try:
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            log.debug(
                f"查询成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return self.package_data(data, col_names)
        except pymysql.Error as e:
            log.error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return None

    # 指定列名和值进行查询
    def select(self, column_name, value):
        return self.select_by_obj({column_name: value})

    # 根据字典对象进行查询
    def select_by_obj(self, obj: dict):
        self.test_conn()
        col_names = self.get_column_names()
        col_params = self.get_col_params(col_names)
        values = self.get_values(obj)
        param = self.get_query_params(obj)
        # print(f"|{col_names}|{col_params}|{values}|{param}|")
        sql = f"SELECT {col_params} FROM {self.table} where {param}"
        try:
            self.cursor.execute(sql, values)
            data = self.cursor.fetchall()
            log.debug(
                f"查询成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return self.package_data(data, col_names)
        except pymysql.Error as e:
            log.error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return None

    # 更新一个属性
    def update(self, column_name: str, new_value, key_column_name: str, key_value):
        return self.update_by_objs(
            {column_name: new_value}, {key_column_name: key_value}
        )

    # 更新对象(需要传入一个字典 列名:值)
    def update_by_obj(self, obj: dict, key_column_name, key_value):
        return self.update_by_objs(obj, {key_column_name: key_value})

    # 更新对象
    def update_by_objs(self, new_obj: dict, obj: dict):
        self.test_conn()
        values = self.get_values(new_obj) + self.get_values(obj)
        set_params = self.get_set_params(new_obj)
        query_params = self.get_query_params(obj)
        sql = f"UPDATE {self.table} set {set_params} where {query_params}"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            log.debug(
                f"更新成功 -> {self.cursor._last_executed} -> 影响行数: {self.cursor.rowcount}"
            )
            return True
        except pymysql.Error as e:
            log.error(f"更新失败 -> {self.cursor._last_executed} -> {str(e)}")
            self.db.rollback()
            return False

    def get_column_names(self):
        self.test_conn()
        sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        try:
            self.cursor.execute(sql, (self.database, self.table))
            res = []
            for row in self.cursor.fetchall():
                res.append(row[0])
            log.debug(f"获取列名成功 -> {self.cursor._last_executed} -> {res}")
            return res
        except pymysql.Error as e:
            log.error(f"获取列名失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False


if __name__ == "__main__":
    db = DB()
    """ exists/exists_by_obj """
    """ print(db.exists("key", "tQ0gK2eM4fR2uD1xK"))
    print(db.exists("key", "tQ0gK2eM4fR2uD1xK1"))
    print(db.exists("sno", None))
    print(db.exists("sno", ""))
    print(
        db.exists_by_obj(
            {"key": "tQ0gK2eM4fR2uD1xK", "sno": "3123358142", "role": "All"}
        )
    )
    print(db.exists_by_obj({"key": "xE2tX7cN8iZ6xQ1uG", "sno": None}))
    print(db.exists_by_obj({"key": "xE2tX7cN8iZ6xQ1uG", "sno": ""}))
    print(db.exists_by_obj({"key1": "xE2tX7cN8iZ6xQ1uG", "sno": ""})) """
    """ insert_row/insert_rows """
    """ obj = {"key": "哈哈哈4441", "sno": "2", "role": 1}
    obj2 = {"key": "哈哈哈4444", "sno": "2", "role": ""}
    obj3 = {"key": "哈哈哈55", "sno": "3", "role": None}
    obj_list = [obj, obj2, obj3]
    # res = db.insert_rows(obj_list)
    # # res = db.insert_many(["key", "sno", "role"], obj_list)

    # print(res) """

    """ delete """
    """ print(db.delete_row("key", "哈哈哈9999999"))
    print(db.delete_row("sno", "2"))
    print(db.delete_row_by_obj({"key": "3", "sno": 2, "role": None}))
    print(db.delete_row_by_obj({"key": "3", "sno": 3, "role": 3}))
    print(db.delete_row_by_obj({"role": "1"}))
    print(db.delete_rows_by_obj(obj_list)) """
    """ # for i in range(10):
    #     db.insert_row({"key": i})
    data = []
    for i in range(0, 30):
        data.append({"key": str(i)})
    # db.insert_rows(data)
    db.delete_many_by_obj(["key"], data) """

    # print(db.select_all())
    # print(db.select("sno", "2"))
    # print(db.select("sno", None))
    # print(db.select("role", None))
    # print(db.select_by_obj({"sno": "2", "role": "123"}))
    # print(db.select_by_obj({"sno": "2", "role": 123}))
    # print(db.select_by_obj({"key": "哈哈哈551"}))

    # obj = {"key": "哈哈哈9999999", "sno": 11111}
    # obj2 = {"key": "哈哈哈7777", "sno": 6666, "role": 1}
    # print(db.update_by_objs(obj, obj2))
    # print(db.update_by_obj({"key": "哈哈哈5521", "sno": 123, "role": 123}, "sno", "2"))
    # print(db.update_by_obj({"key": "哈哈哈111", "sno": 11, "role": 11}, "role", "123"))
    # print(db.update("key", "哈哈哈7777", "sno", "11111"))
    # print(db.update("role", "12313", "sno", None))
    # print(db.get_column_names())
