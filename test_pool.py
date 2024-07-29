from WkMysql import WkMysqlPool
import time
import threading
from WkLog import log

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "test_table"

if __name__ == "__main__":
    pool = WkMysqlPool(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        max_conn=5,
        min_conn=3,
        time_interval=10,
        max_idle_timeout=20,
    )

    def test_pool(name):
        with pool.get_conn() as conn:
            res = conn.set_table(TABLE).select_all()
            log.info(f"{name} -> {pool.pool.qsize()} -> {len(res)}")

    while True:
        tasks = []
        for i in range(20):
            tasks.append(threading.Thread(target=test_pool, args=("task-{}".format(i),)))
        for task in tasks:
            task.start()
        for task in tasks:
            task.join()
        time.sleep(10)
