from WkDB import *
from WkDB.WkDBPool import WkMysqlPool

# 多线程
import threading
import time
import random


def test_multi_thread_select():
    db = DB()
    db.set_table(TABLE)
    start = time.time()

    tasks = []
    for i in range(1000):
        tasks.append(
            threading.Thread(target=db.select, args=({"id": str(random.randint(31, 100))},))
        )

    for task in tasks:
        task.start()

    for task in tasks:
        task.join()

    end = time.time()
    print("用时：", end - start)


def test_multi_thread_select2():
    def select(name):
        print(f"thread {name} start")
        conn = pool.get_conn()
        print(f"thread {name} get conn")
        res = conn.set_table(TABLE).select(sno="444444444444444444")
        # time.sleep(9.5)
        pool.close_all()
        pool.put_conn(conn)
        print(f"thread {name} - {res} - {pool.pool}")

    pool = WkMysqlPool(
        host=HOST,
        user=USER,
        port=PORT,
        password=PASSWORD,
        database=DATABASE,
        max_conn=5,
        min_conn=1,
    )
    start = time.time()

    tasks = []
    for i in range(100):
        tasks.append(threading.Thread(target=select, args=(str(i),)))

    for task in tasks:
        task.start()

    for task in tasks:
        task.join()

    end = time.time()
    print("用时：", end - start)


if __name__ == "__main__":
    pass
    # test_multi_thread_select()
    test_multi_thread_select2()
