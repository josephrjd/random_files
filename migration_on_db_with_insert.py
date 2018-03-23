############################# Connection settings for postgres #############################
p_host = "**"
p_port = "**"
p_user = "**"
p_password = "**"
p_db_name = "**"
############################# End of Connection settings for postgres #######################

############################# Connection settings for hp_verticad #############################
v_host = "**"
v_port = "**"
v_user = "**"
v_password = "**"
v_db_name = "**"
v_schema_name = "**"
############################# End of Connection settings for hp_vertica #############################

############################# Other settings ########################################################
DEBUG = True

tablesToMigrate = ["dim_auth", "master"]

import psycopg2
import datetime

# Establishing connections #
connection_postgres = psycopg2.connect(database=p_db_name,
                                       user=p_user, password=p_password,
                                       host=p_host, port=p_port
                                       )
if (DEBUG):
    connection_vertica = psycopg2.connect(database=v_db_name,
                                          user=v_user, password=v_password,
                                          host=v_host, port=v_port
                                          )
else:
    from vertica_python import connect
    vertica_conn_info = {'host': v_host, 'port': int(v_port), 'user': v_user, 'password': v_password, 'database': v_db_name}
    connection_vertica = connect(**vertica_conn_info)

cursor_postgres = connection_postgres.cursor()
cursor_vertica = connection_vertica.cursor()

# End of establishing connections #

######### Script to take the table and copy to vertica #################
type_str = type('str')
type_datetime = type(datetime.datetime.now())
type_int = type(1)
type_float = type(1.0)
type_None = type(None)


# todo: handle blob data type

def convert2str(record):
    res = []
    for item in record:
        if type(item) == type_None:
            res.append('NULL')
        elif type(item) == type_str:
            item = item.replace("'", "''")
            res.append("'" + item + "'")
        elif type(item) == type_datetime:
            res.append("'" + str(item) + "'")
        elif '-' in str(item):
            res.append("'" + str(item) + "'")
        else:
            res.append(str(item))
    return ','.join(res)


def copy_table(tab_name, src_cursor, dst_cursor, target_schema):
    sql = 'select * from ' + tab_name
    src_cursor.execute(sql)
    res = src_cursor.fetchall(),
    cnt = 0
    row_length = len(res[0])
    for index, record in enumerate(res[0]):
        print("TABLE: ", tab_name, " PROGRESS:", index , "/" , row_length)
        val_str = convert2str(record)
        full_destination = target_schema + "." + tab_name
        sql = 'insert into %s values(%s);' % (full_destination, val_str)
        try:
            dst_cursor.execute(sql)
            cnt += 1
        except Exception as e:
            print(e, sql, tab_name)
            import sys
            sys.exit()
    return cnt

for table in tablesToMigrate:
    if DEBUG:
        cursor_vertica.execute('TRUNCATE ' + v_schema_name + "." + table)
    else:
        cursor_vertica.execute('TRUNCATE TABLE ' + v_schema_name + "." + table)
    copy_table(table, cursor_postgres, cursor_vertica, v_schema_name)
    break;

connection_postgres.close()
connection_vertica.close()