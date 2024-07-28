from WkMysql import DB
import time

db = DB(time_interval=10)


while True:
    res = db.set_table("test_table").select_all()
    print(len(res))
    time.sleep(1)
