/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach to ODS.
# File Version: KPI v1.0
*/


begin;

DROP TABLE if exists bec_ods.CST_COST_HISTORY_V;

CREATE TABLE IF NOT EXISTS bec_ods.CST_COST_HISTORY_V
(
	cost_update_id NUMERIC(15,0)   ENCODE az64
	,cost_update_id_join NUMERIC(15,0)   ENCODE az64
	,count NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,item_description VARCHAR(240)   ENCODE lzo
	,padded_item_number VARCHAR(120)   ENCODE lzo
	,uom VARCHAR(3)   ENCODE lzo
	,update_description VARCHAR(240)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,standard_cost NUMERIC(28,10)   ENCODE az64
	,inv_adj_val_c NUMERIC(28,10)   ENCODE az64
	,intransit_adj_val_c NUMERIC(28,10)   ENCODE az64
	,wip_adj_val_c NUMERIC(28,10)   ENCODE az64
	,inv_adj_qty_c NUMERIC(28,10)   ENCODE az64
	,intransit_adj_qty_c NUMERIC(28,10)   ENCODE az64
	,wip_adj_qty_c NUMERIC(28,10)   ENCODE az64
	,material NUMERIC(28,10)   ENCODE az64
	,material_overhead NUMERIC(28,10)   ENCODE az64
	,resource_cost NUMERIC(28,10)   ENCODE az64
	,outside_processing NUMERIC(28,10)   ENCODE az64
	,overhead NUMERIC(28,10)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
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

INSERT INTO bec_ods.CST_COST_HISTORY_V (
	cost_update_id,
	cost_update_id_join,
	count,
	inventory_item_id,
	item_description,
	padded_item_number,
	uom,
	update_description,
	organization_id,
	update_date,
	standard_cost,
	inv_adj_val_c,
	intransit_adj_val_c,
	wip_adj_val_c,
	inv_adj_qty_c,
	intransit_adj_qty_c,
	wip_adj_qty_c,
	material,
	material_overhead,
	resource_cost,
	outside_processing,
	overhead,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
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
	cost_update_id_join,
	count,
	inventory_item_id,
	item_description,
	padded_item_number,
	uom,
	update_description,
	organization_id,
	update_date,
	standard_cost,
	inv_adj_val_c,
	intransit_adj_val_c,
	wip_adj_val_c,
	inv_adj_qty_c,
	intransit_adj_qty_c,
	wip_adj_qty_c,
	material,
	material_overhead,
	resource_cost,
	outside_processing,
	overhead,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CST_COST_HISTORY_V;

end;


UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_cost_history_v';
	
commit;