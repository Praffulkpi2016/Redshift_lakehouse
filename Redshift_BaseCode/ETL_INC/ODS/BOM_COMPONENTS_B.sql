/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents Incremental load approach for ODS.
# File Version: KPI v1.0
*/
begin;

-- Delete Records

delete from bec_ods.BOM_COMPONENTS_B
where COMPONENT_SEQUENCE_ID in (
select stg.COMPONENT_SEQUENCE_ID
from bec_ods.BOM_COMPONENTS_B ods, bec_ods_stg.BOM_COMPONENTS_B stg
where ods.COMPONENT_SEQUENCE_ID = stg.COMPONENT_SEQUENCE_ID
and stg.kca_operation IN ('INSERT','UPDATE')
);

commit;

-- Insert records

insert into	bec_ods.BOM_COMPONENTS_B
       (operation_seq_num,
	component_item_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	item_num,
	component_quantity,
	component_yield_factor,
	component_remarks,
	effectivity_date,
	change_notice,
	implementation_date,
	disable_date,
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
	planning_factor,
	quantity_related,
	so_basis,
	optional,
	mutually_exclusive_options,
	include_in_cost_rollup,
	check_atp,
	shipping_allowed,
	required_to_ship,
	required_for_revenue,
	include_on_ship_docs,
	include_on_bill_docs,
	low_quantity,
	high_quantity,
	acd_type,
	old_component_sequence_id,
	component_sequence_id,
	bill_sequence_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wip_supply_type,
	pick_components,
	supply_subinventory,
	supply_locator_id,
	operation_lead_time_percent,
	revised_item_sequence_id,
	cost_factor,
	bom_item_type,
	from_end_item_unit_number,
	to_end_item_unit_number,
	original_system_reference,
	eco_for_production,
	enforce_int_requirements,
	component_item_revision_id,
	delete_group_name,
	dg_description,
	optional_on_model,
	parent_bill_seq_id,
	model_comp_seq_id,
	plan_level,
	from_bill_revision_id,
	to_bill_revision_id,
	auto_request_material,
	suggested_vendor_name,
	vendor_id,
	unit_price,
	obj_name,
	pk1_value,
	pk2_value,
	pk3_value,
	pk4_value,
	pk5_value,
	from_end_item_rev_id,
	to_end_item_rev_id,
	overlapping_changes,
	from_object_revision_id,
	from_minor_revision_id,
	to_object_revision_id,
	to_minor_revision_id,
	from_end_item_minor_rev_id,
	to_end_item_minor_rev_id,
	component_minor_revision_id,
	from_structure_revision_code,
	to_structure_revision_code,
	from_end_item_strc_rev_id,
	to_end_item_strc_rev_id,
	basis_type,
	common_component_sequence_id,
	inherit_flag,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)	
(
	select
		operation_seq_num,
	component_item_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	item_num,
	component_quantity,
	component_yield_factor,
	component_remarks,
	effectivity_date,
	change_notice,
	implementation_date,
	disable_date,
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
	planning_factor,
	quantity_related,
	so_basis,
	optional,
	mutually_exclusive_options,
	include_in_cost_rollup,
	check_atp,
	shipping_allowed,
	required_to_ship,
	required_for_revenue,
	include_on_ship_docs,
	include_on_bill_docs,
	low_quantity,
	high_quantity,
	acd_type,
	old_component_sequence_id,
	component_sequence_id,
	bill_sequence_id,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	wip_supply_type,
	pick_components,
	supply_subinventory,
	supply_locator_id,
	operation_lead_time_percent,
	revised_item_sequence_id,
	cost_factor,
	bom_item_type,
	from_end_item_unit_number,
	to_end_item_unit_number,
	original_system_reference,
	eco_for_production,
	enforce_int_requirements,
	component_item_revision_id,
	delete_group_name,
	dg_description,
	optional_on_model,
	parent_bill_seq_id,
	model_comp_seq_id,
	plan_level,
	from_bill_revision_id,
	to_bill_revision_id,
	auto_request_material,
	suggested_vendor_name,
	vendor_id,
	unit_price,
	obj_name,
	pk1_value,
	pk2_value,
	pk3_value,
	pk4_value,
	pk5_value,
	from_end_item_rev_id,
	to_end_item_rev_id,
	overlapping_changes,
	from_object_revision_id,
	from_minor_revision_id,
	to_object_revision_id,
	to_minor_revision_id,
	from_end_item_minor_rev_id,
	to_end_item_minor_rev_id,
	component_minor_revision_id,
	from_structure_revision_code,
	to_structure_revision_code,
	from_end_item_strc_rev_id,
	to_end_item_strc_rev_id,
	basis_type,
	common_component_sequence_id,
	inherit_flag,
        KCA_OPERATION,
       'N' AS IS_DELETED_FLG,
	    cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
		kca_seq_date
	from bec_ods_stg.BOM_COMPONENTS_B
	where kca_operation IN ('INSERT','UPDATE') 
	and (COMPONENT_SEQUENCE_ID,kca_seq_id) in 
	(select COMPONENT_SEQUENCE_ID,max(kca_seq_id) from bec_ods_stg.BOM_COMPONENTS_B 
     where kca_operation IN ('INSERT','UPDATE')
     group by COMPONENT_SEQUENCE_ID)
);

commit;

-- Soft delete
update bec_ods.BOM_COMPONENTS_B set IS_DELETED_FLG = 'N';
commit;
update bec_ods.BOM_COMPONENTS_B set IS_DELETED_FLG = 'Y'
where (COMPONENT_SEQUENCE_ID )  in
(
select COMPONENT_SEQUENCE_ID  from bec_raw_dl_ext.BOM_COMPONENTS_B
where (COMPONENT_SEQUENCE_ID ,KCA_SEQ_ID)
in 
(
select COMPONENT_SEQUENCE_ID ,max(KCA_SEQ_ID) as KCA_SEQ_ID 
from bec_raw_dl_ext.BOM_COMPONENTS_B
group by COMPONENT_SEQUENCE_ID 
) 
and kca_operation= 'DELETE'
);
commit;


end;

update bec_etl_ctrl.batch_ods_info
set	last_refresh_date = getdate()
where ods_table_name = 'bom_components_b';

commit;