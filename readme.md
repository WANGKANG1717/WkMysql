# WkMysql

WkMysql 是一个用于简化与 MySQL 数据库交互的 Python 模块，专为单线程应用设计，适合短时间内快速连接 MySQL 数据库。通过此模块，您可以轻松执行数据库的创建、查询、插入、更新和删除等操作。

## 特点

- **易用性**: 提供简单明了的 API，便于用户进行数据库操作。
- **持久连接**: 自动测试并保持数据库连接的活跃性，减少频繁连接的开销。
- **线程安全**: 使用线程锁确保在多线程环境中安全地操作数据库。
- **详细日志**: 提供操作成功与失败的记录，便于调试和维护。
- **支持事务**: 执行插入、更新及删除操作时支持事务，确保数据一致性。

## 依赖

在使用 WkMysql 之前，请确保安装 `pymysql` 库：

```bash
pip install pymysql
```

## 安装

```python
pip install WkMysql
```

## 使用方法

### 1. 初始化

```python
from WkMysql import WkMysql

db = WkMysql(
    host='localhost',
    user='root',
    password='123456',
    database='myproject'
)
```

### 2. 设置操作表

设置当前操作的表：

```python
db.set_table('test_table')
```

### 3. 创建表

创建新表：

```python
db.create_table({'id': 'INT PRIMARY KEY', 'name': 'VARCHAR(50)'})
```

### 4. 插入数据

插入一行数据：

```python
result, insert_id = db.insert_row({'id': 1, 'name': 'wangkang'})
```

### 5. 查询数据

查询表中的所有数据：

```python
all_data = db.select_all()
```

### 6. 更新数据

更新已有数据：

```python
db.update({'id': 1}, {'name': 'new_name'})
```

### 7. 删除数据

删除特定行：

```python
db.delete_row({'id': 1})
```

### 8. 关闭连接

在程序结束时关闭数据库连接：

```python
db.close()
```

## 示例

以下是一个完整的示例，演示如何使用 WkMysql 包进行常见的数据库操作：

```python
from WkMysql import WkMysql

db = WkMysql(
    host='localhost',
    user='root',
    password='123456',
    database='myproject'
)

db.set_table('test_table')
db.create_table({'id': 'INT PRIMARY KEY', 'name': 'VARCHAR(50)'})
db.insert_row({'id': 1, 'name': 'wangkang'})
results = db.select_all()
print(results)
db.update({'id': 1}, {'name': 'new_name'})
db.delete_row({'id': 1})
```

## 项目地址

项目源代码及文档请访问：[WkMysql 项目](https://gitee.com/purify_wang/wkdb)

## 许可证

本项目遵循 GPL2.0 许可证。请查看 [LICENSE](LICENSE) 文件以获取更多信息。

---

感谢您使用 WkMysql！如有任何问题或建议，请随时联系。
