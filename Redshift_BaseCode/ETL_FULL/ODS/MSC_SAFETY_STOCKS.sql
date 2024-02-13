/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach for ODS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.MSC_SAFETY_STOCKS;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_SAFETY_STOCKS
(
	plan_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,period_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,safety_stock_quantity NUMERIC(28,10)   ENCODE az64
	,updated NUMERIC(15,0)   ENCODE az64
	,status NUMERIC(15,0)   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,target_safety_stock NUMERIC(28,10)   ENCODE az64
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,planning_group VARCHAR(30)   ENCODE lzo
	,user_defined_safety_stocks NUMERIC(28,10)   ENCODE az64
	,user_defined_dos NUMERIC(15,0)   ENCODE az64
	,target_days_of_supply NUMERIC(15,0)   ENCODE az64
	,achieved_days_of_supply NUMERIC(15,0)   ENCODE az64
	,unit_number VARCHAR(30)   ENCODE lzo
	,demand_var_ss_percent NUMERIC(28,10)   ENCODE az64
	,mfg_ltvar_ss_percent NUMERIC(28,10)   ENCODE az64
	,transit_ltvar_ss_percent NUMERIC(28,10)   ENCODE az64
	,sup_ltvar_ss_percent NUMERIC(28,10)   ENCODE az64
	,total_unpooled_safety_stock NUMERIC(28,10)   ENCODE az64
	,item_type_id NUMERIC(15,0)   ENCODE az64
	,item_type_value NUMERIC(15,0)   ENCODE az64
	,new_plan_id NUMERIC(15,0)   ENCODE az64
	,simulation_set_id NUMERIC(15,0)   ENCODE az64
	,new_plan_list VARCHAR(256)   ENCODE lzo
	,applied NUMERIC(15,0)   ENCODE az64
	,reserved_safety_stock_qty NUMERIC(28,10)   ENCODE az64
	,inventory_level NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_SAFETY_STOCKS (
	plan_id,
	organization_id,
	sr_instance_id,
	inventory_item_id,
	period_start_date,
	safety_stock_quantity,
	updated,
	status,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_id,
	program_update_date,
	program_application_id,
	target_safety_stock,
	project_id,
	task_id,
	planning_group,
	user_defined_safety_stocks,
	user_defined_dos,
	target_days_of_supply,
	achieved_days_of_supply,
	unit_number,
	demand_var_ss_percent,
	mfg_ltvar_ss_percent,
	transit_ltvar_ss_percent,
	sup_ltvar_ss_percent,
	total_unpooled_safety_stock,
	item_type_id,
	item_type_value,
	new_plan_id,
	simulation_set_id,
	new_plan_list,
	applied,
	reserved_safety_stock_qty,
	inventory_level,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date)
SELECT
	plan_id,
	organization_id,
	sr_instance_id,
	inventory_item_id,
	period_start_date,
	safety_stock_quantity,
	updated,
	status,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_id,
	program_update_date,
	program_application_id,
	target_safety_stock,
	project_id,
	task_id,
	planning_group,
	user_defined_safety_stocks,
	user_defined_dos,
	target_days_of_supply,
	achieved_days_of_supply,
	unit_number,
	demand_var_ss_percent,
	mfg_ltvar_ss_percent,
	transit_ltvar_ss_percent,
	sup_ltvar_ss_percent,
	total_unpooled_safety_stock,
	item_type_id,
	item_type_value,
	new_plan_id,
	simulation_set_id,
	new_plan_list,
	applied,
	reserved_safety_stock_qty,
	inventory_level,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.MSC_SAFETY_STOCKS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_safety_stocks';