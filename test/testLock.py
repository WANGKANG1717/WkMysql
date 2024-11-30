from WkMysql.WkMysql_new import WkMysql

db = WkMysql(host="127.0.0.1", user="root", password="123456", database="tmp")
db.set_table("tmp1111").create_table(
    {
        "id": "INT PRIMARY KEY AUTO_INCREMENT",
        "key": "varchar(255)",
        "sno": "varchar(255)",
        "role": "varchar(255)",
    },
    delete_if_exists=False,
)
