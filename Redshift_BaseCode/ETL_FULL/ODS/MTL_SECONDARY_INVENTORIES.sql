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
drop table if exists bec_ods.MTL_SECONDARY_INVENTORIES;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_SECONDARY_INVENTORIES
(
	secondary_inventory_name VARCHAR(10)   ENCODE lzo
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,description VARCHAR(50)   ENCODE lzo
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inventory_atp_code NUMERIC(15,0)   ENCODE az64
	,availability_type NUMERIC(15,0)   ENCODE az64
	,reservable_type NUMERIC(15,0)   ENCODE az64
	,locator_type NUMERIC(15,0)   ENCODE az64
	,picking_order NUMERIC(15,0)   ENCODE az64
	,material_account NUMERIC(15,0)   ENCODE az64
	,material_overhead_account NUMERIC(15,0)   ENCODE az64
	,resource_account NUMERIC(15,0)   ENCODE az64
	,overhead_account NUMERIC(15,0)   ENCODE az64
	,outside_processing_account NUMERIC(15,0)   ENCODE az64
	,quantity_tracked NUMERIC(15,0)   ENCODE az64
	,asset_inventory NUMERIC(15,0)   ENCODE az64
	,source_type NUMERIC(15,0)   ENCODE az64
	,source_subinventory VARCHAR(10)   ENCODE lzo
	,source_organization_id NUMERIC(15,0)   ENCODE az64
	,requisition_approval_type NUMERIC(15,0)   ENCODE az64
	,expense_account NUMERIC(15,0)   ENCODE az64
	,encumbrance_account NUMERIC(15,0)   ENCODE az64
	,attribute_category VARCHAR(30)   ENCODE lzo
	,attribute1 VARCHAR(150)   ENCODE lzo
	,attribute2 VARCHAR(150)   ENCODE lzo
	,attribute3 VARCHAR(150)   ENCODE lzo
	,attribute4 VARCHAR(150)   ENCODE lzo
	,attribute5 VARCHAR(150)   ENCODE lzo
	,attribute6 VARCHAR(150)   ENCODE lzo
	,attribute7 VARCHAR(150)   ENCODE lzo
	,attribute8 VARCHAR(150)   ENCODE lzo
	,attribute9 VARCHAR(150)   ENCODE lzo
	,attribute10 VARCHAR(150)   ENCODE lzo
	,attribute11 VARCHAR(150)   ENCODE lzo
	,attribute12 VARCHAR(150)   ENCODE lzo
	,attribute13 VARCHAR(150)   ENCODE lzo
	,attribute14 VARCHAR(150)   ENCODE lzo
	,attribute15 VARCHAR(150)   ENCODE lzo
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,preprocessing_lead_time NUMERIC(15,0)   ENCODE az64
	,processing_lead_time NUMERIC(15,0)   ENCODE az64
	,postprocessing_lead_time NUMERIC(15,0)   ENCODE az64
	,demand_class VARCHAR(30)   ENCODE lzo
	,project_id NUMERIC(15,0)   ENCODE az64
	,task_id NUMERIC(15,0)   ENCODE az64
	,subinventory_usage NUMERIC(15,0)   ENCODE az64
	,notify_list_id NUMERIC(15,0)   ENCODE az64
	,pick_uom_code VARCHAR(3)   ENCODE lzo
	,depreciable_flag NUMERIC(15,0)   ENCODE az64
	,location_id NUMERIC(15,0)   ENCODE az64
	,default_cost_group_id NUMERIC(15,0)   ENCODE az64
	,status_id NUMERIC(15,0)   ENCODE az64
	,default_loc_status_id NUMERIC(15,0)   ENCODE az64
	,lpn_controlled_flag NUMERIC(15,0)   ENCODE az64
	,pick_methodology NUMERIC(15,0)   ENCODE az64
	,cartonization_flag NUMERIC(30,0)   ENCODE az64
	,dropping_order NUMERIC(15,0)   ENCODE az64
	,subinventory_type NUMERIC(15,0)   ENCODE az64
	,planning_level NUMERIC(22,0)   ENCODE az64
	,default_count_type_code NUMERIC(22,0)   ENCODE az64
	,enable_bulk_pick VARCHAR(1)   ENCODE lzo
	,enable_locator_alias VARCHAR(1)   ENCODE lzo
	,enforce_alias_uniqueness VARCHAR(1)   ENCODE lzo
	,enable_opp_cyc_count VARCHAR(1)   ENCODE lzo
	,opp_cyc_count_days NUMERIC(5,0)   ENCODE az64
	,opp_cyc_count_header_id NUMERIC(15,0)   ENCODE az64
	,opp_cyc_count_quantity NUMERIC(28,10)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
	,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

insert into bec_ods.MTL_SECONDARY_INVENTORIES
(
secondary_inventory_name,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	disable_date,
	inventory_atp_code,
	availability_type,
	reservable_type,
	locator_type,
	picking_order,
	material_account,
	material_overhead_account,
	resource_account,
	overhead_account,
	outside_processing_account,
	quantity_tracked,
	asset_inventory,
	source_type,
	source_subinventory,
	source_organization_id,
	requisition_approval_type,
	expense_account,
	encumbrance_account,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	preprocessing_lead_time,
	processing_lead_time,
	postprocessing_lead_time,
	demand_class,
	project_id,
	task_id,
	subinventory_usage,
	notify_list_id,
	pick_uom_code,
	depreciable_flag,
	location_id,
	default_cost_group_id,
	status_id,
	default_loc_status_id,
	lpn_controlled_flag,
	pick_methodology,
	cartonization_flag,
	dropping_order,
	subinventory_type,
	planning_level,
	default_count_type_code,
	enable_bulk_pick,
	enable_locator_alias,
	enforce_alias_uniqueness,
	enable_opp_cyc_count,
	opp_cyc_count_days,
	opp_cyc_count_header_id,
	opp_cyc_count_quantity,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
(
select
secondary_inventory_name,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	description,
	disable_date,
	inventory_atp_code,
	availability_type,
	reservable_type,
	locator_type,
	picking_order,
	material_account,
	material_overhead_account,
	resource_account,
	overhead_account,
	outside_processing_account,
	quantity_tracked,
	asset_inventory,
	source_type,
	source_subinventory,
	source_organization_id,
	requisition_approval_type,
	expense_account,
	encumbrance_account,
	attribute_category,
	attribute1,
	attribute2,
	attribute3,
	attribute4,
	attribute5,
	attribute6,
	attribute7,
	attribute8,
	attribute9,
	attribute10,
	attribute11,
	attribute12,
	attribute13,
	attribute14,
	attribute15,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	preprocessing_lead_time,
	processing_lead_time,
	postprocessing_lead_time,
	demand_class,
	project_id,
	task_id,
	subinventory_usage,
	notify_list_id,
	pick_uom_code,
	depreciable_flag,
	location_id,
	default_cost_group_id,
	status_id,
	default_loc_status_id,
	lpn_controlled_flag,
	pick_methodology,
	cartonization_flag,
	dropping_order,
	subinventory_type,
	planning_level,
	default_count_type_code,
	enable_bulk_pick,
	enable_locator_alias,
	enforce_alias_uniqueness,
	enable_opp_cyc_count,
	opp_cyc_count_days,
	opp_cyc_count_header_id,
	opp_cyc_count_quantity,
	KCA_OPERATION,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date 
from bec_ods_stg.MTL_SECONDARY_INVENTORIES
);

end;

update bec_etl_ctrl.batch_ods_info set load_type = 'I', 
last_refresh_date = getdate() where ods_table_name='mtl_secondary_inventories'; 

commit;