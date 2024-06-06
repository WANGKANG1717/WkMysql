# -*- coding: utf-8 -*-
# @Date     : 2024-05-07 10:45:04
# @Author   : WANGKANG
# @Blog     : https://wangkang1717.github.io
# @Email    : 1686617586@qq.com
# @Filepath : WkMysqlPool.py
# @Brief    : Mysql连接池
# Copyright 2024 WANGKANG, All Rights Reserved.
from WkMysql import DB
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


class WkMysqlPool:
    def __init__(
        self,
        host,
        user,
        password,
        database,
        port,
        max_conn=10,
        min_conn=3,
        connection_timeout=10000,  # 连接超时：10秒
        max_idle_timeout=60000,  # 最大空闲超时：60秒
        **kwargs,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.max_conn: int = max_conn  # 最大连接数
        self.min_conn: int = min_conn  # 最小连接数
        self.connection_timeout: int = connection_timeout / 1000  # 转换为秒
        self.max_idle_timeout: int = max_idle_timeout
        self.kwargs = kwargs

        self.lock = Lock()
        self.pool: Queue = self._init_pool()

    def _init_pool(self):
        pool = Queue(self.max_conn)
        for _ in range(self.min_conn):
            pool.put((self._create_connection(), time.time()))  # 初始化最小连接, 同时记录时间戳
        return pool

    def _create_connection(self) -> DB:
        # print("new_conn")
        return DB(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            **self.kwargs,
        )

    def with_lock(func):
        def wrapper(self, *args, **kwargs):
            with self.lock:
                return func(self, *args, **kwargs)

        return wrapper

    def get_connection(self) -> DB:
        with self.lock:
            print("get_connection")
            if self.pool.empty():
                conn = self._create_connection()
                self.pool.put((conn, time.time()))

            conn, last_time = self.pool.get()
            return conn

    @contextmanager
    def get_conn(self):
        conn = self.get_connection()
        try:
            yield conn  # 提供连接给调用者
        finally:
            # 在上下文退出后释放连接
            self.release_connection(conn)

    def release_connection(self, conn: DB):
        print("release_connection")
        if self.pool.full():
            conn.close()
        self.pool.put((conn, time.time()))


if __name__ == "__main__":
    # start_time = time.time()
    # time.sleep(1.2)
    # print(time.time() - start_time)

    # start_time = time.perf_counter()
    # time.sleep(1.2)
    # print(time.perf_counter() - start_time)
    pool = WkMysqlPool(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        max_conn=5,
        min_conn=3,
    )
    tmp_pool = []
    print(pool.pool)

    def sleep(time_):
        print("sleep", time_)
        time.sleep(time_)
        if not pool.pool:
            conn = tmp_pool.pop()
            print("@@@@@@@@@@@@@@@@")
            pool.put_conn(conn)
            print("$$$$$$$$$$$$")

    for _ in range(10):
        print(_)
        # if _ == 5:
        #     import threading

        #     threading.Thread(target=sleep, args=(1,)).start()
        # conn = pool.get_connection()
        # print("conn:", conn)
        # pool.release_connection(conn)
        # tmp_pool.append(conn)
        # print("tmp_pool:", tmp_pool)
        with pool.get_conn() as conn:
            print("conn:", conn)

    for conn in tmp_pool:
        pool.release_connection(conn)
    print(pool.pool)
    # pool.close_all()
    print(pool.pool)
