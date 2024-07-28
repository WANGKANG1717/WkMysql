from WkLog import INFO
from WkDB import DB
import time

from WkDB import log as db_log

# db_log.level = INFO
# db_log.output_location = 1

db = DB(time_interval=10)

from WkLog import log

while True:
    res = db.set_table("test_table").select_all()
    print(len(res))
    time.sleep(1)
    log.debug("test")
