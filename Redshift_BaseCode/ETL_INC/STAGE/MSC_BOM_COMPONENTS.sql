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
BEGIN;

TRUNCATE TABLE bec_ods_stg.MSC_BOM_COMPONENTS;

insert into	bec_ods_stg.MSC_BOM_COMPONENTS
   (
      plan_id,
	component_sequence_id,
	bill_sequence_id,
	sr_instance_id,
	organization_id,
	inventory_item_id,
	using_assembly_id,
	component_type,
	scaling_type,
	change_notice,
	revision,
	uom_code,
	usage_quantity,
	effectivity_date,
	disable_date,
	from_unit_number,
	to_unit_number,
	use_up_code,
	suggested_effectivity_date,
	driving_item_id,
	operation_offset_percent,
	optional_component,
	old_effectivity_date,
	wip_supply_type,
	planning_factor,
	atp_flag,
	component_yield_factor,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	scale_multiple,
	scale_rounding_variance,
	rounding_direction,
	primary_flag,
	contribute_to_step_qty,
	old_component_sequence_id,
	operation_seq_num,
	new_plan_id,
	new_plan_list,
	applied,
	simulation_set_id,
	base_model_item_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date)
(select
	   plan_id,
	component_sequence_id,
	bill_sequence_id,
	sr_instance_id,
	organization_id,
	inventory_item_id,
	using_assembly_id,
	component_type,
	scaling_type,
	change_notice,
	revision,
	uom_code,
	usage_quantity,
	effectivity_date,
	disable_date,
	from_unit_number,
	to_unit_number,
	use_up_code,
	suggested_effectivity_date,
	driving_item_id,
	operation_offset_percent,
	optional_component,
	old_effectivity_date,
	wip_supply_type,
	planning_factor,
	atp_flag,
	component_yield_factor,
	refresh_number,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
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
	scale_multiple,
	scale_rounding_variance,
	rounding_direction,
	primary_flag,
	contribute_to_step_qty,
	old_component_sequence_id,
	operation_seq_num,
	new_plan_id,
	new_plan_list,
	applied,
	simulation_set_id,
	base_model_item_id,
	KCA_OPERATION,
	kca_seq_id,
	kca_seq_date
from
	bec_raw_dl_ext.MSC_BOM_COMPONENTS
where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= '' 
	and (PLAN_ID,COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID ,KCA_SEQ_ID) in 
	(select PLAN_ID,COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID ,max(KCA_SEQ_ID) from bec_raw_dl_ext.MSC_BOM_COMPONENTS 
     where kca_operation != 'DELETE' and nvl(kca_seq_id,'')!= ''
     group by PLAN_ID,COMPONENT_SEQUENCE_ID, SR_INSTANCE_ID, BILL_SEQUENCE_ID )
     and kca_seq_date > (select (executebegints-prune_days) from bec_etl_ctrl.batch_ods_info where ods_table_name ='msc_bom_components')
	 );
END;

