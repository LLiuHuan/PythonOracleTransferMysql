# -*- coding: utf-8 -*-
import cx_Oracle
import pymysql
import time
import os
import numpy as np
import sys, time
np.set_printoptions(suppress=True)
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

SOURCE_DATABASE = "Oracle"  # 源库 # 不区分大小写 后面转小写了
TARGET_DATABASE = "Mysql"  # 目标库

ERROR_TABLES = []


def switchDatabase(IsDatabase):
    if IsDatabase.lower() == "mysql":
        return pymysql.connect("127.0.0.1", "root", "root", "test")
    elif IsDatabase.lower() == "oracle":
        return cx_Oracle.connect('root/root@127.0.0.1/orcl')


# 源库
source_db = switchDatabase(SOURCE_DATABASE)
# 目标库
target_db = switchDatabase(TARGET_DATABASE)

cur_select = source_db.cursor()  # 源库查询对象
cur_insert = target_db.cursor()  # 目标库插入对象
cur_select.arraysize = 500
cur_insert.arraysize = 500

if SOURCE_DATABASE.lower() == "oracle":
    tables_sql = "select table_name from user_tables"
elif SOURCE_DATABASE.lower() == "mysql":
    tables_sql = "SELECT table_name FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = 'gryrwdb'"
else:
    # 默认Oracle 主要是为了处理提示异常 看着不舒服
    tables_sql = "select table_name from user_tables"

cur_select.execute(tables_sql)
# cols = [i[0] for i in cur_select.description]
tables = cur_select.fetchall()

# 循环表名
for table in tables:
    # table = dict(zip(cols, table))
    # print(table)
    table_name = table[0]
    if table_name in [
            'THRZDAYPARAMETER', 'REALTIMETEMP', 'SALESLIST', 'SYST_EW_NHTJ',
            'SYS_MONTH_EW_NHTJ', 'TB_UNIT_DATA_CURRENT', 'TB_UNIT_DATA_CURRENT_TEST'
    ]:
        continue
    try:
        # table_name = table[0]
        if SOURCE_DATABASE.lower() == "oracle":
            get_column_len = 'select * from ' + table_name + ' where rownum<=1'
        elif SOURCE_DATABASE.lower() == "mysql":
            get_column_len = 'select * from ' + table_name + ' limit 1'
        else:
            # 默认Oracle
            get_column_len = 'select * from ' + table_name + ' where rownum<=1'
        # print(get_column_len)
        cur_select.execute(get_column_len)
        col_len = len(cur_select.fetchone())
        val_str = ''
        if TARGET_DATABASE.lower() == 'mysql':
            # insert into tb_name values(%s,%s,%s,%s)
            for i in range(1, col_len):
                val_str = val_str + '%s' + ','
            val_str = val_str + '%s'
        elif TARGET_DATABASE.lower() == 'oracle':
            # insert into tb_name values(:1,:2,:3)
            for i in range(1, col_len):
                val_str = val_str + ':' + str(i) + ','
            val_str = val_str + ':' + str(col_len)
        # print(val_str)
        insert_sql = 'insert into ' + table_name + ' values(' + val_str + ')'
        select_sql = 'select * from ' + table_name
        cur_select.execute(select_sql)
        print('开始执行插入[%s]:' % table_name,
              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        ii = 0
        c = 10000
        cur_insert.execute("select count(*) con from %s" % table_name)
        con_ins_rows = cur_insert.fetchone()
        cur_select.execute("select count(*) con from %s" % table_name)
        con_sel_row = cur_select.fetchone()
        # print(con_rows)
        if int(con_ins_rows[0]) == int(con_sel_row[0]):
            print('[%s]数据已存在:' % table_name,
                  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            continue
        elif int(con_ins_rows[0]) > 0:
            print('[%s]数据不一致，删除后重新添加:' % table_name,
                  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            cur_insert.execute("delete from %s" % table_name)
            target_db.commit()
        while True:
            ii += 1
            sys.stdout.write(str(ii) + '\r')
            sys.stdout.flush()
            rows = list(cur_select.fetchmany(5555))
            # print(rows)
            cur_insert.executemany(insert_sql, rows)  # 批量插入每次500行
            target_db.commit()  # 提交
            if not rows:
                break  # 中断循环
        print('插入[%s]完成!' % table_name,
              time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), ii * c)
    except Exception as e:
        print(e)
        ERROR_TABLES.append(table_name)
        print('[%s]:' % table_name, '插入失败')
print(ERROR_TABLES)
cur_select.close()
cur_insert.close()
source_db.close()
target_db.close()