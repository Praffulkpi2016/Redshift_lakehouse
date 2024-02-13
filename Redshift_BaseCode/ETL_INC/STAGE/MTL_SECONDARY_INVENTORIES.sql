/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for stage.
# File Version: KPI v1.0
*/
begin;

truncate
	table bec_ods_stg.MTL_SECONDARY_INVENTORIES;

insert
	into
	bec_ods_stg.MTL_SECONDARY_INVENTORIES
(secondary_inventory_name,
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
	kca_operation,
	kca_seq_id,
	kca_seq_date
)
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
	kca_operation,
	kca_seq_id,
	kca_seq_date
	from
		bec_raw_dl_ext.MTL_SECONDARY_INVENTORIES
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= '' 
and (SECONDARY_INVENTORY_NAME,ORGANIZATION_ID,kca_seq_id) in 
(select SECONDARY_INVENTORY_NAME,ORGANIZATION_ID,max(kca_seq_id) from bec_raw_dl_ext.MTL_SECONDARY_INVENTORIES 
where kca_operation != 'DELETE'  and nvl(kca_seq_id,'')!= ''
group by SECONDARY_INVENTORY_NAME,ORGANIZATION_ID)
and 
( kca_seq_date > (
select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name = 'mtl_secondary_inventories')
 
            )
);		
		
end;