from WkLog import log


def before_execute(func):
    def wrapper(*args, **kwargs):
        print(f"before_execute -> {func.__name__}")
        print(f"args: {args}")
        print(f"kwargs: {kwargs}")
        # self.__test_conn()
        if not args and not kwargs:
            log.error(f"{func.__name__} -> 参数不能为空！")
            return False

        if args and kwargs:
            log.error(f"{func.__name__} -> 参数不能同时存在！")

        if args:
            if len(args) > 1:
                log.error(f"{func.__name__} -> 参数个数不能超过1")
            if not isinstance(args[0], dict):
                log.error(f"{func.__name__} -> 参数必须是字典！")
        result = func(*args, **kwargs)
        return result

    return wrapper


@before_execute
def func(x, y, *args, **kwargs):
    print(f"x={x}")
    print(f"y={y}")
    print(f"args={args}")
    print(f"kwargs={kwargs}")
    pass


# def exists(x, y):
#     print(f"exists: {x} - {y}")


# def exists(x: dict):
#     print(f"exists: {x}")


@before_execute
def exists(*args, **kwargs):
    print(f"exists-args: {args} - {True if args else False}")
    print(f"exists-kwargs: {kwargs} - {True if kwargs else False}")


if __name__ == "__main__":
    pass
    # func(x=1, y=2, a=1, b=2, c=3)
    # func(1, 2, a=1, b=2, c=3)
    # func(1, 2, 3, 4, 5, a=1, b=2, c=3)
    # kwargs = {"a": 1, "b": 2, "c": 3}
    # func(1, 2, 3, 4, 5, **kwargs)
    # print(*kwargs)
    # exists(1, 2)
    # exists(1, 2, 3, 4, 5)
    # exists(1)
    # exists({"a": 1, "b": 2})
    # exists(a=1, b=2)
    # exists(id=1, name="wangkang")
    # exists({"id": 1, "name": "wangkang"})
