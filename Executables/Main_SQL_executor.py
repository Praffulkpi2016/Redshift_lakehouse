# Databricks notebook source
import sqlglot,re,os,glob
from sqlglot import exp, parse_one,parse

# COMMAND ----------

# dbutils.widgets.text('tables_list','')
# dbutils.widgets.text('catalog','')
# dbutils.widgets.text('layer','')

# COMMAND ----------

tables_list = dbutils.widgets.get('tables_list')
tables_list = tables_list.split(',')
catalog = dbutils.widgets.get('catalog')
layer = dbutils.widgets.get('layer')

# COMMAND ----------

confg_db = spark.sql("select * from redshift_lakehouse.bec_etl_ctrl.dbk_confg_info")
catalog = confg_db.filter(" config_key = 'catalog' ").select("config_value").collect()[0].config_value
redshift_sqls_path = confg_db.filter(" config_key = 'redshift_sqls_path' ").select("config_value").collect()[0].config_value
transpiled_sqls_path = confg_db.filter(" config_key = 'transpiled_sqls_path' ").select("config_value").collect()[0].config_value

# COMMAND ----------

spark.sql(f"use catalog {catalog}")
batch_ods_info = spark.sql(f"select * from bec_etl_ctrl.batch_ods_info")
batch_dw_info = spark.sql(f"select * from bec_etl_ctrl.batch_dw_info")

# COMMAND ----------

def create_schemas(sql):
    schema_set = set()
    try:
        parsed_file = parse(sql)

        for each_tree in parsed_file:
            if each_tree:
                for table in each_tree.find_all(exp.Table):
                    schema_set.add(table.db)
        print(f"schemas from sql - {schema_set}")
        for each_schema in schema_set:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {each_schema};").show()

    except Exception: pass

def execute_sql(sql_file,file_name,layer,altered_sql=None):
            try:
                sql = open(sql_file,"r").read() if not altered_sql else altered_sql
                if not altered_sql:create_schemas(sql)
                print(f"Executing sql for table - {file_name} ---->")
                for each_statement in sql.split(";"):
                    if each_statement:spark.sql(each_statement).show()
                print(f"Execution Completed\n--------")
            except Exception as e:
                    exc = str(e)
                    if "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in exc:
                        colmn = exc[exc.find("name "):exc.find(" cannot be resolved.")][6:-1]
                        print(f"{file_name=}",f"{layer=}","removing column=",colmn)
                        sql = re.sub(r".*"+colmn+r".*$", '', sql, flags=re.MULTILINE | re.IGNORECASE)
                        # with open(sql_file,'w') as altered_sql: altered_sql.write(sql)
                        # spark.sql(f"alter table {add_column_layer_map[layer]}.{file_name} add column (`{colmn}` string)").show()
                        execute_sql(sql_file,file_name,layer,sql)
                    else:print(file_name,"#######",str(e)[:500])

# COMMAND ----------

spark.sql(f"use catalog {catalog}")
if layer == 'stage':
    full_load_path = transpiled_sqls_path+"/ETL_FULL/STAGE"
    inc_load_path = transpiled_sqls_path+"/ETL_INC/STAGE"
    full_files_list = glob.glob(f"{full_load_path}/*.sql", recursive=True)
    inc_files_list = glob.glob(f"{inc_load_path}/*.sql", recursive=True)
    for each_table in tables_list:
        disable_flag = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('disable_flag').collect()[0].disable_flag
        if not disable_flag:
            load_type = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('load_type').collect()[0].load_type
            if load_type == 'F':
                for each_file in full_files_list:
                    if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
                        print(f'Table={each_table}, {load_type=}')
                        execute_sql(each_file,each_table,layer)
                        break
                else: print(f'{layer} full sql not available for {each_table}')
            elif load_type == 'I':
                for each_file in inc_files_list:
                    if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
                        print(f'Table={each_table}, {load_type=}')
                        execute_sql(each_file,each_table,layer)
                        break
                else: print(f'{layer} incremental sql not available for {each_table}')
        else: print(f"Skipping Execution of {each_table} due to {disable_flag=} ")
elif layer == 'ods':
    full_load_path = transpiled_sqls_path+"/ETL_FULL/ODS"
    inc_load_path = transpiled_sqls_path+"/ETL_INC/ODS"
    full_files_list = glob.glob(f"{full_load_path}/*.sql", recursive=True)
    inc_files_list = glob.glob(f"{inc_load_path}/*.sql", recursive=True)
    for each_table in tables_list:
        disable_flag = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('disable_flag').collect()[0].disable_flag
        if not disable_flag:
            load_type = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('load_type').collect()[0].load_type
            if load_type == 'F':
                for each_file in full_files_list:
                    if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
                        print(f'Table={each_table}, {load_type=}')
                        execute_sql(each_file,each_table,layer)
                        break
                else: print(f'{layer} full sql not available for {each_table}')
            elif load_type == 'I':
                for each_file in inc_files_list:
                    if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
                        print(f'Table={each_table}, {load_type=}')
                        execute_sql(each_file,each_table,layer)
                        break
                else: print(f'{layer} incremental sql not available for {each_table}')
        else: print(f"Skipping Execution of {each_table} due to {disable_flag=} ")
elif layer == 'analytics':
    full_load_path = transpiled_sqls_path+"/ETL_FULL/ANALYTICS"
    inc_load_path = transpiled_sqls_path+"/ETL_INC/ANALYTICS"
    full_files_list = glob.glob(f"{full_load_path}/*.sql", recursive=True)
    inc_files_list = glob.glob(f"{inc_load_path}/*.sql", recursive=True)
    for each_table in tables_list:
        disable_flag = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('disable_flag').collect()[0].disable_flag
        if not disable_flag:
            load_type = batch_dw_info.filter(f"dw_table_name = '{each_table}' ").select('load_type').collect()[0].load_type
            if load_type == 'F':
                for each_file in full_files_list:
                    if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
                        print(f'Table={each_table}, {load_type=}')
                        execute_sql(each_file,each_table,layer)
                        break
                else: print(f'{layer} full sql not available for {each_table}')
            elif load_type == 'I':
                for each_file in inc_files_list:
                    if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
                        print(f'Table={each_table}, {load_type=}')
                        execute_sql(each_file,each_table,layer)
                        break
                else: print(f'{layer} incremental sql not available for {each_table}')
        else: print(f"Skipping Execution of {each_table} due to {disable_flag=} ")
# elif layer == 'rds':
#     full_load_path = transpiled_sqls_path+"/ETL_FULL/RDS"
#     inc_load_path = transpiled_sqls_path+"/ETL_INC/RDS"
#     full_files_list = glob.glob(f"{full_load_path}/*.sql", recursive=True)
#     inc_files_list = glob.glob(f"{inc_load_path}/*.sql", recursive=True)
#     for each_table in tables_list:
#         disable_flag = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('disable_flag').collect()[0].disable_flag
#         if not disable_flag:
#             load_type = batch_ods_info.filter(f"ods_table_name = '{each_table}' ").select('load_type').collect()[0].load_type
#             if load_type == 'F':
#                 for each_file in full_files_list:
#                     if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
#                         print(f'Table={each_table}, {load_type=}')
#                         execute_sql(each_file,each_table,layer)
#                         break
#                 else: print(f'{layer} full sql not available for {each_table}')
#             elif load_type == 'I':
#                 for each_file in inc_files_list:
#                     if each_table == os.path.basename(each_file).replace(".sql","").replace("Transpiled_","").lower():
#                         print(f'Table={each_table}, {load_type=}')
#                         execute_sql(each_file,each_table,layer)
#                         break
#                 else: print(f'{layer} incremental sql not available for {each_table}')
#         else: print(f"Skipping Execution of {each_table} due to {disable_flag=} ")
            
