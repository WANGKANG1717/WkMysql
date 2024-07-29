from WkMysql import WkMysql
from pymysql.cursors import Cursor

HOST = "localhost"
PORT = 3306
USER = "root"
PASSWORD = "123456"
DATABASE = "myproject"
TABLE = "test_table"

if __name__ == "__main__":
    # db = DB(cursorclass=Cursor)
    db = WkMysql()
    db.set_table(TABLE)
    # db.execute_many("INSERT INTO test_table(`key`, sno) VALUES(%s, %s)", [[1, "test"], [2, "test2"]])
    # db.execute_many("UPDATE test_table SET `key`=%s WHERE sno=%s", [[10, "test"], [30, "test2"]])
    # db.set_table(TABLE)
    # res = db.execute("UPDATE `test_table` SET `key`=%s WHERE `sno`=%s", [22, "0"])
    # print(res)
    # res = db.update({"id": 1111}, {"id": 1})
    # print(res)
    # print(db.select_all())
    # print(db.select_one(id="31"))
    # print(db.select_one(key="哈哈哈7777"))
    """ exists/exists_by_obj """
    # db.set_table(TABLE)
    # print(db.set_table("gym_reserve").get_column_names())
    # print(db.exists(key="tQ0gK2eM4fR2uD1xK"))
    # print(db.exists(key="tQ0gK2eM4fR2uD1xK1"))
    # print(db.exists(sno=None))
    # print(db.set_table(TABLE).exists(sno=""))
    # print(db.set_table(TABLE).exists({"key": "tQ0gK2eM4fR2uD1xK", "sno": "3123358142", "role": "All"}))
    # print(db.set_table(TABLE).exists({"key": "xE2tX7cN8iZ6xQ1uG", "sno": None}))
    # print(db.set_table(TABLE).exists({"key": "xE2tX7cN8iZ6xQ1uG", "sno": ""}))
    # print(db.set_table(TABLE).exists({"key1": "xE2tX7cN8iZ6xQ1uG", "sno": ""}))
    """ insert_row/insert_rows """
    """ db.set_table(TABLE)
    obj = {"key": "哈哈哈4441", "sno": "2222222222222222222", "role": 1}
    obj2 = {"key": "哈哈哈4444", "sno": "333333333333333333333", "role": ""}
    obj3 = {"key": "哈哈哈55", "sno": "444444444444444444' or 1=1", "role": None}
    id = db.insert_row(obj2)
    print(id)
    obj_list = [obj, obj2, obj3]
    res = db.insert_rows(obj_list)
    print(res)
    res = db.set_table(TABLE).insert_row(id=22, key="wangkang", sno="3123358142", role="All")
    print(res)
    res = db.insert_many(obj_list)
    print(res) """

    """ delete """
    """ obj = {"key": "哈哈哈4441", "sno": "2", "role": 1}
    obj2 = {"key": "哈哈哈4444", "sno": "2", "role": ""}
    obj3 = {"key": "哈哈哈55", "sno": "3", "role": None}
    obj_list = [obj, obj2, obj3]
    print(db.set_table(TABLE).insert_many(obj_list))
    print(db.delete_row(key="哈哈哈4444"))
    print(db.delete_row(sno="2"))
    print(db.delete_row({"key": "3", "sno": 2, "role": None}))
    print(db.delete_row({"key": "3", "sno": 3, "role": 3}))
    print(db.delete_row({"role": "1"}))
    print(db.delete_rows(obj_list))
    for i in range(10):
        print(db.insert_row({"key": i}))
    """
    """ data = []
    for i in range(0, 30):
        data.append({"key": str(i), "sno": str(i), "role": str(i)})
    print(db.insert_rows(data))
    print(db.delete_many(data))

    db.set_table(TABLE)
    print(db.select_all())
    print(db.select(sno=2))
    print(db.select(sno=None))
    print(db.select(role=None))
    print(db.select({"sno": "2", "role": "123"}))
    print(db.select({"sno": "2", "role": 123}))
    print(db.select({"key": "哈哈哈551"}))

    obj = {"sno": "12"}
    obj2 = {"key": "哈哈哈7777", "sno": "112", "role": "1"}
    print(db.update(obj, obj2)) """

    # 测试create_table
    # data = {
    #     "id": "INT PRIMARY KEY AUTO_INCREMENT",
    #     "label": "varchar(255)",
    #     "_id": "varchar(255)",
    #     "creator": "varchar(255)",
    #     "updater": "varchar(255)",
    #     "createTime": "varchar(255)",
    #     "updateTime": "varchar(255)",
    #     "userAgent": "varchar(255)",
    #     "flowDecision": "varchar(255)",
    #     "_widget_1616673287156": "varchar(255)",
    #     "_widget_1679462572706": "varchar(255)",
    #     "_widget_1617678690020": "varchar(255)",
    #     "_widget_1615827614721": "varchar(255)",
    #     "_widget_1615835360787": "varchar(255)",
    #     "_widget_1615835360820": "varchar(255)",
    #     "_widget_1615868277748": "varchar(255)",
    #     "_widget_1679382295430": "varchar(255)",
    #     "_widget_1680158790676": "varchar(255)",
    #     "_widget_1615827613948": "varchar(255)",
    #     "_widget_1615827614024": "varchar(255)",
    #     "_widget_1616127228841": "varchar(255)",
    #     "_widget_1646551883542": "varchar(255)",
    #     "_widget_1615827614179": "varchar(255)",
    #     "_widget_1615828535162": "varchar(255)",
    #     "_widget_1615827614346": "text",
    #     "_widget_1615853253200": "varchar(255)",
    #     "_widget_1646552160980": "varchar(255)",
    #     "_widget_1615827614230": "varchar(255)",
    #     "_widget_1650676573176": "varchar(255)",
    #     "_widget_1616138014817": "varchar(255)",
    #     "_widget_1679319585604": "varchar(255)",
    #     "_widget_1645530113180": "text",
    #     "_widget_1615827614500": "varchar(255)",
    #     "_widget_1615827614519": "varchar(255)",
    #     "_widget_1615827614556": "varchar(255)",
    #     "_widget_1679206290832": "varchar(255)",
    #     "_widget_1679206291318": "varchar(255)",
    #     "_widget_1646573100387": "varchar(255)",
    #     "_widget_1646573100578": "varchar(255)",
    #     "_widget_1616161492340": "varchar(255)",
    #     "_widget_1646573100763": "varchar(255)",
    #     "_widget_1646573103096": "varchar(255)",
    #     "_widget_1615827614467": "varchar(255)",
    #     "_widget_1615868277437": "varchar(255)",
    #     "_widget_1679206291570": "varchar(255)",
    #     "_widget_1679206291997": "varchar(255)",
    #     "_widget_1679206292059": "varchar(255)",
    #     "_widget_1615827614316": "text",
    #     "_widget_1615872450096": "varchar(255)",
    #     "_widget_1615827614331": "text",
    #     "_widget_1615872450115": "varchar(255)",
    #     "_widget_1646573101933": "text",
    #     "_widget_1646573101968": "varchar(255)",
    #     "_widget_1679206292413": "text",
    #     "_widget_1679206292466": "varchar(255)",
    #     "_widget_1646573101063": "varchar(255)",
    #     "_widget_1617845711912": "varchar(255)",
    #     "_widget_1616138013810": "varchar(255)",
    #     "_widget_1710314345323": "varchar(255)",
    #     "_widget_1710314345885": "varchar(255)",
    #     "_widget_1710327754686": "varchar(255)",
    #     "_widget_1710329993986": "varchar(255)",
    #     "chargers_name": "varchar(255)",
    #     "appId": "varchar(255)",
    #     "entryId": "varchar(255)",
    # }

    """ data = {
        "id": "INT PRIMARY KEY AUTO_INCREMENT",
        "key": "varchar(255)",
        "sno": "varchar(255)",
        "role": "varchar(255)",
    }
    db.set_table("test").create_table(data, True)
    db.set_table("test").insert_row({"key": "123", "sno": "456", "role": "789"})
    # db.set_table("test").delete_table() """
