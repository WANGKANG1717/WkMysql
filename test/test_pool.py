from WkMysql import WkMysqlPool
from WkMysql import WkMysql
import time
import threading
from WkLog import log

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "test_table"


def multi_thread_test():
    pool = WkMysqlPool(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        max_conn=30,
        min_conn=10,
        time_interval=60,
        max_idle_timeout=60 * 60,
    )

    def test_pool(name):
        with pool.get_conn() as conn:
            res = conn.set_table(TABLE).select_all()
            log.info(f"{name} -> {pool.pool.qsize()} -> {len(res)} -> {pool.current_conn}")
            return

    time_start = time.time()
    num = 20
    while True:
        tasks = []
        for i in range(num):
            tasks.append(threading.Thread(target=test_pool, args=("task-{}".format(i),)))
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()
        # time.sleep(1)
        num -= 1
        if num == 0:
            break
        print(num)
    time_end = time.time()
    print("time cost: ", time_end - time_start)


def single_thread_test_pool():
    pool = WkMysqlPool(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        max_conn=30,
        min_conn=10,
        time_interval=60,
        max_idle_timeout=60 * 60,
    )

    def test_pool(name):
        with pool.get_conn() as conn:
            res = conn.set_table(TABLE).select_all()
            log.info(f"{name} -> {pool.pool.qsize()} -> {len(res)} -> {pool.current_conn}")
            return

    time_start = time.time()
    num = 20
    while True:
        for i in range(num):
            test_pool(name="task-{}".format(i))
        # time.sleep(1)
        num -= 1
        if num == 0:
            break
        print(num)
    time_end = time.time()
    print("time cost: ", time_end - time_start)


def single_thread_test():
    db = WkMysql(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        time_interval=10,
    )

    def test_pool(name):
        res = db.set_table(TABLE).select_all()
        log.info(f"{name} ->  {len(res)}")
        return

    time_start = time.time()
    num = 20
    while True:
        for i in range(num):
            test_pool(name="task-{}".format(i))
        # time.sleep(1)
        num -= 1
        if num == 0:
            break
        print(num)
    time_end = time.time()
    print("time cost: ", time_end - time_start)


if __name__ == "__main__":
    multi_thread_test()
    # single_thread_test()
    # single_thread_test_pool()
