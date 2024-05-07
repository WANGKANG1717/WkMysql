import threading

currentRunThreadNumber = 0
maxSubThreadNumber = 10


def task():
    global currentRunThreadNumber
    thName = threading.currentThread().name

    condLock.acquire()  # 上锁
    print("start and wait run thread : %s" % thName)

    condLock.wait()  # 暂停线程运行、等待唤醒
    currentRunThreadNumber += 1
    print("carry on run thread : %s" % thName)

    condLock.release()  # 解锁


if __name__ == "__main__":
    condLock = threading.Condition()

    for i in range(maxSubThreadNumber):
        subThreadIns = threading.Thread(target=task)
        subThreadIns.start()

    while currentRunThreadNumber < maxSubThreadNumber:
        notifyNumber = int(
            input("Please enter the number of threads that need to be notified to run：")
        )

        condLock.acquire()
        condLock.notify(notifyNumber)  # 放行
        condLock.release()

    print("main thread run end")

# 先启动10个子线程，然后这些子线程会全部变为等待状态
# start and wait run thread : Thread-1
# start and wait run thread : Thread-2
# start and wait run thread : Thread-3
# start and wait run thread : Thread-4
# start and wait run thread : Thread-5
# start and wait run thread : Thread-6
# start and wait run thread : Thread-7
# start and wait run thread : Thread-8
# start and wait run thread : Thread-9
# start and wait run thread : Thread-10

# 批量发送通知，放行特定数量的子线程继续运行
# Please enter the number of threads that need to be notified to run：5  # 放行5个
# carry on run thread : Thread-4
# carry on run thread : Thread-3
# carry on run thread : Thread-1
# carry on run thread : Thread-2
# carry on run thread : Thread-5

# Please enter the number of threads that need to be notified to run：5  # 放行5个
# carry on run thread : Thread-8
# carry on run thread : Thread-10
# carry on run thread : Thread-6
# carry on run thread : Thread-9
# carry on run thread : Thread-7

# Please enter the number of threads that need to be notified to run：1
# main thread run end
