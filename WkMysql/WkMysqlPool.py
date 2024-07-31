# -*- coding: utf-8 -*-
# @Date     : 2024-05-07 10:45:04
# @Author   : WANGKANG
# @Blog     : https://wangkang1717.github.io
# @Email    : 1686617586@qq.com
# @Filepath : WkMysqlPool.py
# @Brief    : WkMysql连接池
# Copyright 2024 WANGKANG, All Rights Reserved.

""" 
项目地址：https://gitee.com/purify_wang/wkdb
"""

from .WkMysql import WkMysql
import time
from queue import Queue
from threading import Condition, Lock, Thread
from contextlib import contextmanager

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "test_table"


class WkMysqlPool:
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

        self.conditionLock = Condition()
        self.pool: Queue = self._init_pool()

        self.current_conn = self.pool.qsize()  # 当前连接数
        # 启动空闲连接清理线程
        self.new_thread(self.cleanup_idle_threads)

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

    def _create_connection(self) -> WkMysql:
        return WkMysql(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            **self.kwargs,
        )

    def _get_connection(self) -> WkMysql:
        with self.conditionLock:
            try:
                while self.pool.empty() and self.current_conn >= self.max_conn:
                    self.conditionLock.wait()  # 等待连接池中有空闲连接
                if self.pool.empty():
                    self.current_conn += 1
                    return self._create_connection()
                else:
                    conn, _ = self.pool.get()
                    return conn
            except Exception as e:
                print(f"Failed to get connection: {e}")
                return None

    @contextmanager
    def get_conn(self):
        conn = self._get_connection()
        try:
            yield conn  # 提供连接给调用者
        finally:
            # 在上下文退出后释放连接
            self.release_connection(conn)

    def release_connection(self, conn: WkMysql):
        with self.conditionLock:
            try:
                self.pool.put((conn, time.time()))
            except Exception as e:
                print(f"Failed to release connection: {e}")
            finally:
                self.conditionLock.notify()  # 通知其他线程有空闲连接

    def close_connections(self, conn):
        with self.conditionLock:
            try:
                conn.close()  # 关闭空闲连接
            except Exception as e:
                print(f"Failed to close connection: {e}")
            finally:
                self.current_conn -= 1
                self.conditionLock.notify()

    def new_thread(self, func, *args):
        t = Thread(target=func, args=args)
        t.daemon = True
        t.start()

    def cleanup_idle_threads(self):
        while True:
            time.sleep(self.max_idle_timeout)
            print("cleanup_idle_threads")
            temp_pool = []
            while not self.pool.empty():
                conn, last_use_time = self.pool.get()
                if time.time() - last_use_time > self.max_idle_timeout and self.current_conn > self.min_conn:
                    self.close_connections(conn)
                    print(f"Closed idle connection: {conn}")
                else:
                    temp_pool.append((conn, last_use_time))
            for conn, last_use_time in temp_pool:
                self.release_connection(conn)
