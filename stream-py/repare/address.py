import pymysql
import traceback

def insert_data():
    # 连接数据库，增加autocommit=False确保事务控制
    connection = pymysql.connect(
        host='cdh03',
        port=3306,
        user='root',
        password='root',
        database='realtime_v2',
        charset='utf8mb4',
        autocommit=False
    )
    cursor = connection.cursor()

    try:
        # 验证连接字符集
        cursor.execute("SHOW VARIABLES LIKE 'character_set_%';")
        print("当前连接字符集设置:")
        for row in cursor.fetchall():
            print(row)

        with open('national-code.txt', 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    data = line.strip().split(',')
                    code = data[0]
                    province = data[1] if len(data) > 1 else ''
                    city = data[2] if len(data) > 2 else ''
                    area = data[3] if len(data) > 3 else ''

                    # 打印可能有问题的数据行
                    if any('\u4e00' <= char <= '\u9fff' for char in province):
                        print(f"处理第 {line_num} 行: {province}")

                    sql = "INSERT INTO spider_national_code_compare_dic (code, province, city, area) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql, (code, province, city, area))

                except Exception as e:
                    print(f"第 {line_num} 行插入失败: {line.strip()}")
                    print(f"错误信息: {e}")
                    traceback.print_exc()
                    # 可以选择继续处理后续行，或者直接抛出异常终止
                    # raise

        connection.commit()
        print("数据插入成功！")
    except FileNotFoundError:
        print("文件未找到！")
    except Exception as e:
        print(f"发生错误: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    insert_data()