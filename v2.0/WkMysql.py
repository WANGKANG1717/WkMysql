# -*- coding: utf-8 -*-
# @Date     : 2023-10-13 13:00:00
# @Author   : WangKang
# @Blog     : kang17.xyz
# @Email    : 1686617586@qq.com
# @Filepath : WkMysql.py
# @Brief    : 封装数据库操作
# Copyright 2023 WANGKANG, All Rights Reserved.

import pymysql
from WkLog import log

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
            log.Info("连接数据库成功")
        except Exception as e:
            log.Error(f"连接数据库失败 -> {str(e)}")
            exit(0)

    # 长连接时，如果长时间不进行数据库交互，连接就会关闭，再次请求就会报错
    # 每次使用游标的时候，都调用下这个方法
    def test_conn(self):
        # 这两种方法本质上是一致的
        self.db.ping(reconnect=True)
        # try:
        #     self.db.ping()
        # except:
        #     self.connect_db()

    # 根据关键词判断某一列是否存在
    def exists(self, column_name, value):
        self.test_conn()
        sql = f"select 1 from {self.table} where `{column_name}` = %s limit 1"  # 考虑到列名可能有的比较特殊，因此需要使用``括起来
        try:
            self.cursor.execute(sql, value)
            return self.cursor.fetchone() != None
        except pymysql.Error as e:
            log.Error(
                f"查询失败 -> {self.cursor._last_executed} -> {str(e)}"
            )  # 打印实际执行的sql语句及异常信息
            return False

    # 根据字典对象判断元素是否存在
    def exists_by_obj(self, obj: dict):
        self.test_conn()
        values = tuple(obj.values())
        param = "and ".join([f"`{column_name}` = %s" for column_name in obj.keys()])
        sql = f"select 1 from {self.table} where {param} limit 1"
        try:
            self.cursor.execute(sql, values)
            return self.cursor.fetchone() != None
        except pymysql.Error as e:
            log.Error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 插入单行数据
    def insert_row(self, obj: dict):
        self.test_conn()
        print("insert_row")
        values = tuple(obj.values())
        param1 = ", ".join([f"`{column_name}`" for column_name in obj.keys()])
        param2 = ", ".join(["%s"] * len(obj.keys()))
        sql = f"INSERT INTO {self.table}({param1}) VALUES({param2})"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            return True
        except pymysql.Error as e:
            log.Error(f"添加失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 插入多行数据
    # 不会因为个别数据的添加失败导致后面的所有数据都插入失败
    # 返回成功次数和失败次数
    def insert_rows(self, obj_list: list[dict]):
        self.test_conn()
        if not obj_list:
            return
        success = 0
        fail = 0
        for obj in obj_list:
            if self.insert_row(obj):
                success += 1
            else:
                fail += 1
        return {"success": success, "fail": fail}

    # 列名和值
    def delete_row(self, column_name, value):
        self.test_conn()
        sql = f"DELETE FROM {self.table} WHERE `{column_name}` = %s"
        try:
            self.cursor.execute(sql, value)
            self.db.commit()
            return True
        except pymysql.Error as e:
            log.Error(f"删除失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 字典对象
    def delete_row_by_obj(self, obj: dict):
        self.test_conn()
        values = tuple(obj.values())
        param = "and ".join([f"`{column_name}` = %s" for column_name in obj.keys()])
        sql = f"DELETE FROM {self.table} WHERE {param}"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            return True
        except pymysql.Error as e:
            log.Error(f"删除失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 查询所有数据
    def select_all(self):
        self.test_conn()
        sql = f"SELECT * FROM {self.table}"
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except pymysql.Error as e:
            log.Error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 指定列名和值进行查询
    def select(self, column_name, value):
        self.test_conn()
        sql = f"SELECT * FROM {self.table} where `{column_name}` = %s"
        try:
            self.cursor.execute(sql, value)
            return self.cursor.fetchall()
        except pymysql.Error as e:
            log.Error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 根据字典对象进行查询
    def select_by_obj(self, obj: dict):
        self.test_conn()
        values = tuple(obj.values())
        param = "and ".join([f"`{column_name}` = %s" for column_name in obj.keys()])
        sql = f"SELECT * FROM {self.table} where {param}"
        try:
            self.cursor.execute(sql, values)
            return self.cursor.fetchall()
        except pymysql.Error as e:
            log.Error(f"查询失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False

    # 更新一个属性
    def update(self, column_name: str, new_value, key_column_name: str, key_value):
        self.test_conn()
        sql = f"UPDATE {self.table} set `{column_name}` = %s where `{key_column_name}` = %s"
        try:
            self.cursor.execute(sql, (new_value, key_value))
            self.db.commit()
            print(self.cursor._last_executed)
            return True
        except pymysql.Error as e:
            log.Error(f"数据更新失败 -> {self.cursor._last_executed} -> {str(e)}")
            self.db.rollback()
            return False

    # 更新对象(需要传入一个字典 列名:值)
    def update_by_obj(self, obj: dict, key_column_name, key_value):
        self.test_conn()
        values = list(obj.values()) + [key_value]
        param = ", ".join([f"`{column_name}` = %s" for column_name in obj.keys()])
        sql = f"UPDATE {self.table} set {param} where `{key_column_name}` = %s"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            return True
        except pymysql.Error as e:
            log.Error(f"数据更新失败 -> {self.cursor._last_executed} -> {str(e)}")
            self.db.rollback()
            return False

    # 更新对象
    def update_by_objs(self, new_obj: dict, old_obj: dict):
        self.test_conn()
        values = list(new_obj.values()) + list(old_obj.values())
        param1 = ", ".join([f"`{column_name}` = %s" for column_name in new_obj.keys()])
        param2 = "and ".join(
            [f"`{column_name}` = %s" for column_name in old_obj.keys()]
        )
        sql = f"UPDATE {self.table} set {param1} where {param2}"
        try:
            self.cursor.execute(sql, values)
            self.db.commit()
            return True
        except pymysql.Error as e:
            log.Error(f"数据更新失败 -> {self.cursor._last_executed} -> {str(e)}")
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
            return res
        except pymysql.Error as e:
            log.Error(f"获取列名失败 -> {self.cursor._last_executed} -> {str(e)}")
            return False


if __name__ == "__main__":
    db = DB()
    # obj = {"key": "哈哈哈9999999", "sno": "6666"}
    # obj2 = {"key": "哈哈哈7777", "sno": 6666}
    # print(db.update_by_objs(obj, obj2))
    # print(db.select({"key": "哈哈哈4441"}))
    # print(db.delete_row({"key": "哈哈哈55", "sno": 3}))
    # print(db.exists({"key": "哈哈哈4444", "sno": "2"}))
    # print(db.delete_row("key", "1111"))
    # obj = {"key": "哈哈哈4441", "sno": "2"}
    # obj2 = {"key": "哈哈哈4444", "sno": "2"}
    # obj3 = {"key": "哈哈哈55", "sno": "3"}
    # obj_list = [obj, obj2, obj3]
    # res = db.insert_rows(obj_list)
    # print(res)
    # print(db.select_by("sno", "2222222"))
    # print(db.update_by_obj(obj, "key", "哈哈哈4441"))
    # print(db.get_column_names())
    # print(db.exists("key", "lL8fL1eL4kX8lQ7lD"))
    # print(db.exists("key", "lL8fL1eL4kX8lQ7lD1"))
    # print(db.select_all())
    # print(db.update("sno", "31222222222", "key", "哈哈哈444"))
    # print(db.update_one_property("sno", "31222222222", "key", "lL8fL1eL4kX8lQ7lD"))
