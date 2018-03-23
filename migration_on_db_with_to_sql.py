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
DEBUG = False

tablesToMigrate = ["master", "dim_auth_employee"]

import psycopg2
import datetime
import sqlalchemy as sq
import pandas as pd
import time

start_time = time.time()

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


if DEBUG:
    vertica_engine = sq.create_engine("postgresql+psycopg2://" + p_user + ":" + p_password + "@" + p_host + ":" + str(p_port) + "/" + p_db_name,client_encoding='utf8')
else:
    vertica_engine = sq.create_engine("vertica+vertica_python://" + v_user + ":" + v_password + "@" + v_host + ":" + str(v_port) + "/" + v_db_name)
cursor_postgres = connection_postgres.cursor()
cursor_vertica = connection_vertica.cursor()

# End of establishing connections #

######### Script to take the table and copy to vertica #################
def convert_column_to_int(df, columns):
    for column in columns:
        df[column] = df[column].fillna(0.0).astype(int)
    return df

def set_value_in_na_fields(df, column, value):
    df[column] = df[column].fillna(value)
    return df

def copy_tables(table, table_in_vertica, cursor_postgres, cursor_vertica, v_schema_name):
    cursor_postgres.execute("SELECT * from " + table)
    rows = cursor_postgres.fetchall()
    dataframe = pd.DataFrame(rows, columns=[desc[0] for desc in cursor_postgres.description])
    # Adding all the exception conditions
    try:
        del dataframe['modified']
        del dataframe['version']
    except Exception as e:
        print (e)
    for column in dataframe.columns:
        try:
            dataframe = convert_column_to_int(dataframe, [column])
        except:
            print('')
    if 'data_of_joining' in dataframe.columns:
         dataframe = set_value_in_na_fields(dataframe, 'data_of_joining', '01-01-1970')
    if table == 'sur_map_prev_org':
        dataframe['prev_org_name'] = dataframe['prev_org_name'].str.replace(r'\\$', '')
    if table == 'sur_master_employee_two':
        dataframe = set_value_in_na_fields(dataframe, 'data_of_birth', '01-01-1970')
        dataframe = set_value_in_na_fields(dataframe, 'start_date_current_country', '01-01-1970')
    print("Copying table ", table, " to ", table_in_vertica, "in vertica")
    dataframe.to_sql(table_in_vertica,
                     vertica_engine,
                     schema=v_schema_name,
                     index=False,
                     if_exists='append')


for table in tablesToMigrate:
    table_in_vertica = '' + table
    if DEBUG:
        cursor_vertica.execute('TRUNCATE ' + v_schema_name + "." + table_in_vertica)
    else:
        cursor_vertica.execute('TRUNCATE TABLE ' + v_schema_name + "." + table_in_vertica)
    connection_vertica.commit()
    copy_tables(table, table_in_vertica, cursor_postgres, cursor_vertica, v_schema_name)

connection_postgres.close()
connection_vertica.close()

end_time = time.time()
print("Total time elapsed for migration : ",end_time-start_time)
