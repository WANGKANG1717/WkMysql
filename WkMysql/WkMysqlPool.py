# -*- coding: utf-8 -*-
# @Date     : 2024-05-07 10:45:04
# @Author   : WANGKANG
# @Blog     : https://wangkang1717.github.io
# @Email    : 1686617586@qq.com
# @Filepath : WkMysqlPool.py
# @Brief    : Mysql连接池
# Copyright 2024 WANGKANG, All Rights Reserved.

""" 
项目地址：https://gitee.com/purify_wang/wkmysql
"""

from .WkMysql import WkDB
import time
from queue import Queue
from threading import Lock
from contextlib import contextmanager

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "test_table"


class WkDBPool:
    def __init__(
        self,
        host,
        user,
        password,
        database,
        port,
        min_conn=3,
        max_conn=10,
        max_idle_timeout=60 * 60,  # 最大空闲超时：1小时
        **kwargs,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.max_conn: int = max_conn  # 最大连接数
        self.min_conn: int = min_conn  # 最小连接数
        self.max_idle_timeout: int = max_idle_timeout  # 单位：秒
        self.kwargs = kwargs

        self.lock = Lock()
        self.pool: Queue = self._init_pool()

    def _init_pool(self):
        pool = Queue(self.max_conn)
        for _ in range(self.min_conn):
            try:
                conn = self._create_connection()
                pool.put((conn, time.time()))  # 初始化最小连接, 同时记录时间戳
            except Exception as e:
                print(f"Failed to create initial connection: {e}")
                continue
        return pool

    def _create_connection(self) -> WkDB:
        # print("new_conn")
        return WkDB(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            **self.kwargs,
        )

    def get_connection(self) -> WkDB:
        with self.lock:
            try:
                if self.pool.empty():
                    conn = self._create_connection()
                else:
                    conn, last_time = self.pool.get()
                    if time.time() - last_time > self.max_idle_timeout:
                        conn.close()
                        conn = self._create_connection()
                return conn
            except Exception as e:
                print(f"Failed to get connection: {e}")
                return None

    @contextmanager
    def get_conn(self):
        conn = self.get_connection()
        try:
            yield conn  # 提供连接给调用者
        finally:
            # 在上下文退出后释放连接
            self.release_connection(conn)

    def release_connection(self, conn: WkDB):
        with self.lock:
            try:
                if self.pool.full():
                    conn.close()
                else:
                    self.pool.put((conn, time.time()))
            except Exception as e:
                print(f"Failed to release connection: {e}")
