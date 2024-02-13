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
DROP TABLE if exists bec_ods.BOM_COMPONENTS_B;
CREATE TABLE IF NOT EXISTS bec_ods.BOM_COMPONENTS_B
(
	operation_seq_num NUMERIC(15,0)   ENCODE az64
	,component_item_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,item_num NUMERIC(15,0)   ENCODE az64
	,component_quantity NUMERIC(28,10)   ENCODE az64
	,component_yield_factor NUMERIC(28,10)   ENCODE az64
	,component_remarks VARCHAR(240)   ENCODE lzo
	,effectivity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,change_notice VARCHAR(10)   ENCODE lzo
	,implementation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,planning_factor NUMERIC(15,0)   ENCODE az64
	,quantity_related NUMERIC(15,0)   ENCODE az64
	,so_basis NUMERIC(15,0)   ENCODE az64
	,optional NUMERIC(15,0)   ENCODE az64
	,mutually_exclusive_options NUMERIC(15,0)   ENCODE az64
	,include_in_cost_rollup NUMERIC(15,0)   ENCODE az64
	,check_atp NUMERIC(15,0)   ENCODE az64
	,shipping_allowed NUMERIC(15,0)   ENCODE az64
	,required_to_ship NUMERIC(15,0)   ENCODE az64
	,required_for_revenue NUMERIC(15,0)   ENCODE az64
	,include_on_ship_docs NUMERIC(15,0)   ENCODE az64
	,include_on_bill_docs NUMERIC(15,0)   ENCODE az64
	,low_quantity NUMERIC(15,0)   ENCODE az64
	,high_quantity NUMERIC(15,0)   ENCODE az64
	,acd_type NUMERIC(15,0)   ENCODE az64
	,old_component_sequence_id NUMERIC(15,0)   ENCODE az64
	,component_sequence_id NUMERIC(15,0)   ENCODE az64
	,bill_sequence_id NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wip_supply_type NUMERIC(15,0)   ENCODE az64
	,pick_components NUMERIC(15,0)   ENCODE az64
	,supply_subinventory VARCHAR(10)   ENCODE lzo
	,supply_locator_id NUMERIC(15,0)   ENCODE az64
	,operation_lead_time_percent NUMERIC(28,10)   ENCODE az64
	,revised_item_sequence_id NUMERIC(15,0)   ENCODE az64
	,cost_factor NUMERIC(28,10)   ENCODE az64
	,bom_item_type NUMERIC(15,0)   ENCODE az64
	,from_end_item_unit_number VARCHAR(30)   ENCODE lzo
	,to_end_item_unit_number VARCHAR(30)   ENCODE lzo
	,original_system_reference VARCHAR(50)   ENCODE lzo
	,eco_for_production NUMERIC(15,0)   ENCODE az64
	,enforce_int_requirements NUMERIC(15,0)   ENCODE az64
	,component_item_revision_id NUMERIC(15,0)   ENCODE az64
	,delete_group_name VARCHAR(10)   ENCODE lzo
	,dg_description VARCHAR(240)   ENCODE lzo
	,optional_on_model NUMERIC(15,0)   ENCODE az64
	,parent_bill_seq_id NUMERIC(15,0)   ENCODE az64
	,model_comp_seq_id NUMERIC(15,0)   ENCODE az64
	,plan_level NUMERIC(15,0)   ENCODE az64
	,from_bill_revision_id NUMERIC(15,0)   ENCODE az64
	,to_bill_revision_id NUMERIC(15,0)   ENCODE az64
	,auto_request_material VARCHAR(1)   ENCODE lzo
	,suggested_vendor_name VARCHAR(240)   ENCODE lzo
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,unit_price NUMERIC(28,10)   ENCODE az64
	,obj_name VARCHAR(30)   ENCODE lzo
	,pk1_value VARCHAR(240)   ENCODE lzo
	,pk2_value VARCHAR(240)   ENCODE lzo
	,pk3_value VARCHAR(240)   ENCODE lzo
	,pk4_value VARCHAR(240)   ENCODE lzo
	,pk5_value VARCHAR(240)   ENCODE lzo
	,from_end_item_rev_id NUMERIC(15,0)   ENCODE az64
	,to_end_item_rev_id NUMERIC(15,0)   ENCODE az64
	,overlapping_changes NUMERIC(15,0)   ENCODE az64
	,from_object_revision_id NUMERIC(15,0)   ENCODE az64
	,from_minor_revision_id NUMERIC(15,0)   ENCODE az64
	,to_object_revision_id NUMERIC(15,0)   ENCODE az64
	,to_minor_revision_id NUMERIC(15,0)   ENCODE az64
	,from_end_item_minor_rev_id NUMERIC(15,0)   ENCODE az64
	,to_end_item_minor_rev_id NUMERIC(15,0)   ENCODE az64
	,component_minor_revision_id NUMERIC(15,0)   ENCODE az64
	,from_structure_revision_code VARCHAR(30)   ENCODE lzo
	,to_structure_revision_code VARCHAR(30)   ENCODE lzo
	,from_end_item_strc_rev_id NUMERIC(15,0)   ENCODE az64
	,to_end_item_strc_rev_id NUMERIC(15,0)   ENCODE az64
	,basis_type NUMERIC(15,0)   ENCODE az64
	,common_component_sequence_id NUMERIC(15,0)   ENCODE az64
	,inherit_flag NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;
INSERT INTO bec_ods.BOM_COMPONENTS_B (
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
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
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
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.BOM_COMPONENTS_B;
end;	
	
UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'bom_components_b';
	
commit;