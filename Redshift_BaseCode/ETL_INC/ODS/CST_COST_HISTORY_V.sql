/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents incremental load approach to ODS.
# File Version: KPI v1.0
*/
begin;

truncate table bec_ods.CST_COST_HISTORY_V;

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