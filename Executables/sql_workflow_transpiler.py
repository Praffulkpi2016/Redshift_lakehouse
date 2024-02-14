# Databricks notebook source
import sqlglot,re,os,glob,requests,json
from sqlglot import exp, parse_one,parse

# COMMAND ----------

# dbutils.widgets.text("batch_name","")
# batchname = dbutils.widgets.get("batch_name")
# dbutils.widgets.dropdown(choices=['Y','N'], name="Transpile_sqls_flag",defaultValue="N")
# transpile_flag = dbutils.widgets.get("Transpile_sqls_flag")
# dbutils.widgets.text('catalog','')
# catalog = dbutils.widgets.get('catalog')
# dbutils.widgets.text('existing_cluster_id','')
# existing_cluster_id = dbutils.widgets.get('existing_cluster_id')

# COMMAND ----------

confg_db = spark.sql(f"select * from redshift_lakehouse.bec_etl_ctrl.dbk_confg_info")
catalog = confg_db.filter(" config_key = 'catalog' ").select("config_value").collect()[0].config_value
transpile_flag = confg_db.filter(" config_key = 'transpile_sqls' ").select("config_value").collect()[0].config_value
dbk_url = confg_db.filter(" config_key = 'dbk_url' ").select("config_value").collect()[0].config_value
existing_cluster_id = confg_db.filter(" config_key = 'existing_cluster_id' ").select("config_value").collect()[0].config_value
batchname = confg_db.filter(" config_key = 'batch_name' ").select("config_value").collect()[0].config_value
api_pat_skey = confg_db.filter(" config_key = 'api_pat_skey' ").select("config_value").collect()[0].config_value
stg_sch_name = confg_db.filter(" config_key = 'stg_sch_name' ").select("config_value").collect()[0].config_value
ods_sch_name = confg_db.filter(" config_key = 'ods_sch_name' ").select("config_value").collect()[0].config_value
analy_sch_name = confg_db.filter(" config_key = 'analy_sch_name' ").select("config_value").collect()[0].config_value
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
redshift_sqls_path = confg_db.filter(" config_key = 'redshift_sqls_path' ").select("config_value").collect()[0].config_value
transpiled_sqls_path = confg_db.filter(" config_key = 'transpiled_sqls_path' ").select("config_value").collect()[0].config_value
main_sql_exec_path = confg_db.filter(" config_key = 'main_sql_exec_path' ").select("config_value").collect()[0].config_value
bridge_tasks_path = confg_db.filter(" config_key = 'bridge_tasks_path' ").select("config_value").collect()[0].config_value

api_pat = dbutils.secrets.get(scope='redshift', key=api_pat_skey)

# COMMAND ----------

# MAGIC %md
# MAGIC <h6> Redshift to Databricks SQL Transpiler

# COMMAND ----------

if transpile_flag == 'Y':
    def redshift_databricks_sql_transpiler(input_file_path,output_file_path,transpiled_file_name):
        pre_unsupported_keywords = {
            "COMMIT;": ";",
            "END;": ";",
            "BEGIN;": ";",
            r"analyse.*$": ";",
            r"analyze.*$": ";",
            r"DISTSTYLE\s+(ALL|KEY|EVEN|AUTO)": "",
            r"interleaved\s+\SORTKEY\s*\([^)]*\)": "",
            r"SORTKEY\s*\([^)]*\)": "",
            r"ENCODE\s+(RAW|AZ64|BYTEDICT|DELTA|DELTA32K|LZO|MOSTLY8|MOSTLY16|MOSTLY32|RUNLENGTH|TEXT255|TEXT32K|ZSTD)": "",
            r"distkey\s*\([^)]*\)": "",
            r"\s+#\s+":"",
            "truncate":"truncate table",
            "truncate table table": "truncate table",
            r"varchar\s*\([^)]*\)": "string",
            "trunc\(":"floor(",
            r"bec_dwh_rpt\.":r"gold_bec_dwh_rpt."
        }

        post_unsupported_keywords = {
            "bec_ods_stg.":stg_sch_name,
            "bec_ods.":ods_sch_name,
            "bec_dwh.":analy_sch_name
        }


        with open(input_file_path, "r") as in_file:
            statements = in_file.read()

        for keyword, replacement in pre_unsupported_keywords.items():
            statements = re.sub(keyword, replacement, statements, flags=re.MULTILINE | re.IGNORECASE)

        try:
            transpiled_sql = sqlglot.transpile(statements, read='redshift', write='databricks', pretty=True)
            final_statements=[]
            for lines in transpiled_sql:
                if lines:
                    final_statements.append(lines)
            converted_script=";\n".join(final_statements) + ";"

            for keyword, replacement in post_unsupported_keywords.items():
                converted_script = converted_script.replace(keyword,replacement)

            with open(output_file_path+transpiled_file_name, 'w') as out_file:
                out_file.write(converted_script)

        except Exception as e:
            print(os.path.basename(input_file_path), '\n', '#######', str(e))

    #--Transpiling the SQLs
    base_path = redshift_sqls_path+'/**/*.sql'
    # base_path = '/Workspace/Users/shailesh.r@kpipartners.com/ETL_SCRIPTS/Redshift Code/ETL_FULL/ODS/**.sql'
    # base_path = '/Workspace/Users/shailesh.r@kpipartners.com/ETL_SCRIPTS/Redshift Code/ETL_FULL/ANALYTICS/**.sql'
    original_subdirectory_paths=glob.glob(base_path, recursive=True)
    for path in original_subdirectory_paths:
        input_file_path=path
        abs_target_path=input_file_path.replace(redshift_sqls_path,transpiled_sqls_path)
        transpiled_file_name='Transpiled_'+abs_target_path.split('/')[-1]
        output_file_path='/'.join(abs_target_path.split('/')[:-1])+'/'
        if not os.path.exists(os.path.dirname(output_file_path)):
            os.makedirs(os.path.dirname(output_file_path))
        redshift_databricks_sql_transpiler(input_file_path,output_file_path,transpiled_file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC <h6> Get tables for STAGE, ODS and ANALYTICS

# COMMAND ----------

ods_table_list = ''
dim_table_dict = {}
fact_table_dict = {}
rt_table_dict = {}

batchname_li = batchname.split(',')
batchname_w_p = tuple(batchname_li) if len(batchname_li) >1 else f"('{batchname_li[0]}')"

records = spark.sql(f"select ods_table_name from {catalog}.bec_etl_ctrl.batch_ods_info where refresh_frequency = 'Daily' and batch_name in {batchname_w_p}").collect()
files_list = glob.glob(transpiled_sqls_path+"/ETL_FULL/ODS/*.sql", recursive=True)
for o_table in records:
    ods_table_list = ods_table_list + o_table[0] + ','
print (f"{ods_table_list=}")

analytics_records = spark.sql(f"select dw_table_name,object_level from {catalog}.bec_etl_ctrl.batch_dw_info where refresh_frequency = 'Daily' and batch_name in {batchname_w_p}").collect()
for a_table in analytics_records:
    if a_table[1] == '1':
        dim_table_dict.update({a_table[0]: a_table[1]})
    elif a_table[1] == '2':
        fact_table_dict.update({a_table[0]: a_table[1]})
    elif a_table[1] == '3':
        rt_table_dict.update({a_table[0]: a_table[1]})

# COMMAND ----------

final_dict = dict()

for key, val in fact_table_dict.items():
    if val not in final_dict:
        final_dict[val] = {"fact": key +','}
    elif val in final_dict and "fact" not in final_dict[val].keys():
        final_dict[val].update({'fact': key + ','})
    else:
        final_dict[val]['fact'] += key + ','

for key, val in dim_table_dict.items():
    if val not in final_dict:
        final_dict[val] = {"dim": key + ','}
    elif val in final_dict and "dim" not in final_dict[val].keys():
        final_dict[val].update({'dim': key + ','})
    else:
        final_dict[val]['dim']+= key + ','

for key, val in rt_table_dict.items():
    if val not in final_dict:
        final_dict[val] = {"rt": key + ','}
    elif val in final_dict and "rt" not in final_dict[val].keys():
        final_dict[val].update({'rt': key + ','})
    else:
        final_dict[val]['rt']+= key + ','


final_dict

# COMMAND ----------

stg_ods_tbls_cnt = len(records*2)+2
dim_tbl_cnt = len(dim_table_dict.keys())+len(set(dim_table_dict.values()))
fact_tbl_cnt = len(fact_table_dict.keys())+len(set(fact_table_dict.values()))
rt_table_cnt = len(rt_table_dict.keys())+len(set(rt_table_dict.values()))
ttl_tbls_cnt = stg_ods_tbls_cnt+dim_tbl_cnt+fact_tbl_cnt+rt_table_cnt

print(f'{stg_ods_tbls_cnt=}')
print(f'Analytics_tbls_cnt={dim_tbl_cnt+fact_tbl_cnt+rt_table_cnt}')
print(f'{dim_tbl_cnt=}')
print(f'{fact_tbl_cnt=}')
print(f'{rt_table_cnt=}')
print(f'{ttl_tbls_cnt=}')


split_workflow = True if ttl_tbls_cnt>100 else False
print(f"{split_workflow=}")

# COMMAND ----------

# MAGIC %md
# MAGIC <h6> Generate and create DBK Workflow JSON functions

# COMMAND ----------

def job_json_builder(task_key,workflow_task,base_params=None,dep_on=None,exst_clstr_id=None,job_id=None):
    json_dict = {}

    json_dict.update({"task_key": task_key,"run_if": "ALL_SUCCESS"})

    if workflow_task == 'Main_SQL_executor': 
        json_dict.update({"notebook_task": {
                    "notebook_path": main_sql_exec_path,
                    "source": "WORKSPACE"
                }})
    elif workflow_task == 'bridge_tasks':
        json_dict.update({"notebook_task": {
        "notebook_path": bridge_tasks_path,
        "source": "WORKSPACE"
    }})
    elif workflow_task == 'run_job_task':
        json_dict.update({"run_job_task": {"job_id": job_id}})
    
    if base_params:
        json_dict["notebook_task"].update({"base_parameters": base_params})

    if dep_on:
        json_dict.update({"depends_on":dep_on})

    if exst_clstr_id:
        json_dict.update({"existing_cluster_id": exst_clstr_id})
        

    return json_dict

def create_workflow_fn(wrkflow_name,tasks_li):
  payload = {
    "run_as": {"user_name": "shailesh.r@kpipartners.com"},
    "name": wrkflow_name.replace(',','_')+"_workflow",
    "max_concurrent_runs": 1,
    "webhook_notifications": {},
    "tasks": tasks_li,
    "format": "MULTI_TASK"
  }

  headers = {"Authorization": f"Bearer {api_pat}","Content-Type": "application/json"}
  response = requests.post(f"{dbk_url}/api/2.1/jobs/create", headers=headers, data=json.dumps(payload))

  if response.status_code == 200:
      job_id = eval(response.text)['job_id']
      print(f"Job created successfully - {job_id=}")
      print(f"{dbk_url}/?o=747007169892122#job/{job_id}")
      return job_id
  else:
      print("Error creating job: {}".format(response.text))

# COMMAND ----------

# MAGIC %md
# MAGIC <h6> Workflow JSON feeder

# COMMAND ----------

if not split_workflow:
    tasks_json = []

    stg_ods_tables = ods_table_list[:-1].split(",")

    for stg_tbl in stg_ods_tables:
        tasks_json.append(job_json_builder(f"stg_{stg_tbl}","Main_SQL_executor",{"catalog": catalog, "layer":'stage', "tables_list":stg_tbl},exst_clstr_id=existing_cluster_id))
    tasks_json.append(job_json_builder("stg_load_success","bridge_tasks",dep_on=[{"task_key":f"stg_{tbl}"} for tbl in stg_ods_tables],exst_clstr_id=existing_cluster_id))

    for ods_tbl in stg_ods_tables:
        tasks_json.append(job_json_builder(f"ods_{ods_tbl}","Main_SQL_executor",{"catalog": catalog, "layer":'ods', "tables_list":ods_tbl},{"task_key": "stg_load_success"},existing_cluster_id))
    tasks_json.append(job_json_builder("ods_load_success","bridge_tasks",dep_on=[{"task_key":f"ods_{tbl}"} for tbl in stg_ods_tables],exst_clstr_id=existing_cluster_id))
    prv_load_success = 'ods_load_success'

    for batch in sorted(final_dict.keys()):
        if 'dim' in final_dict[batch].keys():
            table_split_list = final_dict[batch]['dim'][:-1].split(',')
            for each_table in table_split_list:
                tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},dep_on={"task_key":prv_load_success},exst_clstr_id=existing_cluster_id))
            prv_load_success = f"batch_{batch}_dim_load_success"
            tasks_json.append(job_json_builder(prv_load_success,"bridge_tasks",dep_on=[{"task_key":f"batch_{batch}_{tbl}"} for tbl in table_split_list],exst_clstr_id=existing_cluster_id))

    for batch in sorted(final_dict.keys()):
        if 'fact' in final_dict[batch].keys():
            table_split_list = final_dict[batch]['fact'][:-1].split(',')
            for each_table in table_split_list:
                tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},dep_on={"task_key":prv_load_success},exst_clstr_id=existing_cluster_id))
            prv_load_success = f"batch_{batch}_fact_load_success"
            tasks_json.append(job_json_builder(prv_load_success,"bridge_tasks",dep_on=[{"task_key":f"batch_{batch}_{tbl}"} for tbl in table_split_list],exst_clstr_id=existing_cluster_id))
            
    for batch in sorted(final_dict.keys()):
        if 'rt' in final_dict[batch].keys():
            table_split_list = final_dict[batch]['rt'][:-1].split(',')
            for each_table in table_split_list:
                tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},dep_on={"task_key":prv_load_success},exst_clstr_id=existing_cluster_id))               
            prv_load_success = f"batch_{batch}_rt_load_success"
            tasks_json.append(job_json_builder(prv_load_success,"bridge_tasks",dep_on=[{"task_key":f"batch_{batch}_{tbl}"} for tbl in table_split_list],exst_clstr_id=existing_cluster_id))
            
    create_workflow_fn(f"{batchname}",tasks_json)

else:
    tasks_json = []

    stg_ods_tables = ods_table_list[:-1].split(",")

    for batch in sorted(final_dict.keys()):
        if 'dim' in final_dict[batch].keys():
            table_split_list = final_dict[batch]['dim'][:-1].split(',')
            for each_table in table_split_list:
                if batch>'1':
                    tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},dep_on={"task_key":prv_load_success},exst_clstr_id=existing_cluster_id))
                else:
                    tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},exst_clstr_id=existing_cluster_id))
            prv_load_success = f"batch_{batch}_dim_load_success"
            tasks_json.append(job_json_builder(prv_load_success,"bridge_tasks",dep_on=[{"task_key":f"batch_{batch}_{tbl}"} for tbl in table_split_list],exst_clstr_id=existing_cluster_id))

    for batch in sorted(final_dict.keys()):
        if 'fact' in final_dict[batch].keys():
            table_split_list = final_dict[batch]['fact'][:-1].split(',')
            for each_table in table_split_list:
                tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},dep_on={"task_key":prv_load_success},exst_clstr_id=existing_cluster_id))
            prv_load_success = f"batch_{batch}_fact_load_success"
            tasks_json.append(job_json_builder(prv_load_success,"bridge_tasks",dep_on=[{"task_key":f"batch_{batch}_{tbl}"} for tbl in table_split_list],exst_clstr_id=existing_cluster_id))

    for batch in sorted(final_dict.keys()):
        if 'rt' in final_dict[batch].keys():
            table_split_list = final_dict[batch]['rt'][:-1].split(',')
            for each_table in table_split_list:
                tasks_json.append(job_json_builder(f"batch_{batch}_{each_table}",workflow_task="Main_SQL_executor",base_params={"catalog": catalog, "layer":'analytics', "tables_list":each_table},dep_on={"task_key":prv_load_success},exst_clstr_id=existing_cluster_id))
            prv_load_success = f"batch_{batch}_rt_load_success"
            tasks_json.append(job_json_builder(prv_load_success,"bridge_tasks",dep_on=[{"task_key":f"batch_{batch}_{tbl}"} for tbl in table_split_list],exst_clstr_id=existing_cluster_id))
    analytics_job_wflow_id = create_workflow_fn(f"{batchname}_gold_analytics",tasks_json)
    tasks_json.clear()

    for ods_tbl in stg_ods_tables:
        tasks_json.append(job_json_builder(f"ods_{ods_tbl}","Main_SQL_executor",base_params={"catalog": catalog, "layer":'ods', "tables_list":ods_tbl},exst_clstr_id=existing_cluster_id))
    tasks_json.append(job_json_builder("ods_load_success_trgr_analytics","run_job_task",dep_on=[{"task_key":f"ods_{tbl}"} for tbl in stg_ods_tables],job_id=analytics_job_wflow_id))
    stg_job_wflow_id = create_workflow_fn(f"{batchname}_silver_ods",tasks_json)
    tasks_json.clear()

    for stg_tbl in stg_ods_tables:
        tasks_json.append(job_json_builder(f"stg_{stg_tbl}","Main_SQL_executor",base_params={"catalog": catalog, "layer":'stage', "tables_list":stg_tbl},exst_clstr_id=existing_cluster_id))
    tasks_json.append(job_json_builder("stg_load_success_trgr_ods","run_job_task",dep_on=[{"task_key":f"stg_{tbl}"} for tbl in stg_ods_tables],job_id=stg_job_wflow_id))
    stg_wflow_jobid = create_workflow_fn(f"{batchname}_bronze_stg",tasks_json)

