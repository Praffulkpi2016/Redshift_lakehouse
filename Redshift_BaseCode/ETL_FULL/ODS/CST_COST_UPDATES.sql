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

DROP TABLE if exists bec_ods.CST_COST_UPDATES;

CREATE TABLE IF NOT EXISTS bec_ods.CST_COST_UPDATES
(
	cost_update_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,status NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,cost_type_id NUMERIC(15,0)   ENCODE az64
	,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,description VARCHAR(240)   ENCODE lzo
	,range_option NUMERIC(15,0)   ENCODE az64
	,update_resource_ovhd_flag NUMERIC(15,0)   ENCODE az64
	,update_activity_flag NUMERIC(15,0)   ENCODE az64
	,snapshot_saved_flag NUMERIC(15,0)   ENCODE az64
	,inv_adjustment_account NUMERIC(15,0)   ENCODE az64
	,single_item NUMERIC(15,0)   ENCODE az64
	,item_range_low VARCHAR(240)   ENCODE lzo
	,item_range_high VARCHAR(240)   ENCODE lzo
	,category_id NUMERIC(15,0)   ENCODE az64
	,category_set_id NUMERIC(15,0)   ENCODE az64
	,inventory_adjustment_value NUMERIC(28,10)   ENCODE az64
	,intransit_adjustment_value NUMERIC(28,10)   ENCODE az64
	,wip_adjustment_value NUMERIC(28,10)   ENCODE az64
	,scrap_adjustment_value NUMERIC(28,10)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CST_COST_UPDATES (
	cost_update_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	status,
	organization_id,
	cost_type_id,
	update_date,
	description,
	range_option,
	update_resource_ovhd_flag,
	update_activity_flag,
	snapshot_saved_flag,
	inv_adjustment_account,
	single_item,
	item_range_low,
	item_range_high,
	category_id,
	category_set_id,
	inventory_adjustment_value,
	intransit_adjustment_value,
	wip_adjustment_value,
	scrap_adjustment_value,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT
	cost_update_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	status,
	organization_id,
	cost_type_id,
	update_date,
	description,
	range_option,
	update_resource_ovhd_flag,
	update_activity_flag,
	snapshot_saved_flag,
	inv_adjustment_account,
	single_item,
	item_range_low,
	item_range_high,
	category_id,
	category_set_id,
	inventory_adjustment_value,
	intransit_adjustment_value,
	wip_adjustment_value,
	scrap_adjustment_value,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CST_COST_UPDATES;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_cost_updates';
	
commit;