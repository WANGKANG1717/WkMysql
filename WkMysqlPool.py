# -*- coding: utf-8 -*-
# @Date     : 2024-05-07 10:45:04
# @Author   : WANGKANG
# @Blog     : https://wangkang1717.github.io
# @Email    : 1686617586@qq.com
# @Filepath : WkMysqlPool.py
# @Brief    : Mysql连接池
# Copyright 2024 WANGKANG, All Rights Reserved.
from WkMysql import DB
from threading import Condition
import time

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
        idle_timeout=60000,  # 空闲超时：60秒
        **kwargs,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.max_conn = max_conn
        self.min_conn = min_conn
        self.connection_timeout = connection_timeout / 1000  # 转换为秒
        self.idle_timeout = idle_timeout
        self.kwargs = kwargs

        self.pool = self.init_pool()
        self.curr_conn = len(self.pool)
        self.lock = Condition()

    def init_pool(self):
        pool = []
        for _ in range(self.min_conn):
            pool.append(self.new_conn())
        return pool

    def with_lock(func):
        def wrapper(self, *args, **kwargs):
            with self.lock:
                return func(self, *args, **kwargs)

        return wrapper

    def new_conn(self) -> DB:
        # print("new_conn")
        return DB(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            **self.kwargs,
        )

    @with_lock
    def get_conn(self) -> DB:
        # print("get_conn")
        if not self.pool:
            if self.curr_conn < self.max_conn:
                conn = self.new_conn()
                self.curr_conn += 1
                return conn
            else:
                start_time = time.time()
                self.lock.wait(self.connection_timeout)  # 等待空闲超时
                wait_time = time.time() - start_time
                if wait_time > self.connection_timeout:
                    raise TimeoutError(f"获取连接超时!")
                return self.get_conn()

        return self.pop_conn()

    @with_lock
    def put_conn(self, conn: DB):
        # print("put_conn", conn)
        self.pool.append(conn)
        self.lock.notify(1)

    @with_lock
    def pop_conn(self):
        # print("pop_conn")
        return self.pool.pop()

    @with_lock
    def close(self, conn: DB):
        # print("close", conn)
        conn.close_db()
        self.curr_conn -= 1

    @with_lock
    def close_all(self):
        # print("close_all")
        while self.pool:
            conn = self.pool.pop()
            conn.close_db()
            self.curr_conn -= 1
            # print(self.curr_conn, "=========")


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
        time.sleep(time_)
        if not pool.pool:
            conn = tmp_pool.pop()
            pool.put_conn(conn)

    for _ in range(10):
        print(_)
        if _ == 5:
            import threading

            threading.Thread(target=sleep, args=(0.5,)).start()
        conn = pool.get_conn()
        print(conn)
        tmp_pool.append(conn)

    for conn in tmp_pool:
        pool.put_conn(conn)
    print(pool.pool)
    pool.close_all()
    print(pool.pool)
