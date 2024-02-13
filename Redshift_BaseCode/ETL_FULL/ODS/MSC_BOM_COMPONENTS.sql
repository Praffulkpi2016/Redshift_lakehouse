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

DROP TABLE if exists bec_ods.MSC_BOM_COMPONENTS;

CREATE TABLE IF NOT EXISTS bec_ods.MSC_BOM_COMPONENTS
(
	 plan_id NUMERIC(15,0)   ENCODE az64
	,component_sequence_id NUMERIC(15,0)   ENCODE az64
	,bill_sequence_id NUMERIC(15,0)   ENCODE az64
	,sr_instance_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,using_assembly_id NUMERIC(15,0)   ENCODE az64
	,component_type NUMERIC(28,10)   ENCODE az64
	,scaling_type NUMERIC(15,0)   ENCODE az64
	,change_notice VARCHAR(10)   ENCODE lzo
	,revision VARCHAR(3)   ENCODE lzo
	,uom_code VARCHAR(3)   ENCODE lzo
	,usage_quantity NUMERIC(28,10)   ENCODE az64
	,effectivity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,disable_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,from_unit_number VARCHAR(30)   ENCODE lzo
	,to_unit_number VARCHAR(30)   ENCODE lzo
	,use_up_code NUMERIC(15,0)   ENCODE az64
	,suggested_effectivity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,driving_item_id NUMERIC(15,0)   ENCODE az64
	,operation_offset_percent NUMERIC(28,10)   ENCODE az64
	,optional_component NUMERIC(15,0)   ENCODE az64
	,old_effectivity_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,wip_supply_type NUMERIC(15,0)   ENCODE az64
	,planning_factor NUMERIC(15,0)   ENCODE az64
	,atp_flag NUMERIC(15,0)   ENCODE az64
	,component_yield_factor NUMERIC(28,10)   ENCODE az64
	,refresh_number NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
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
	,scale_multiple NUMERIC(28,10)   ENCODE az64
	,scale_rounding_variance NUMERIC(28,10)   ENCODE az64
	,rounding_direction NUMERIC(15,0)   ENCODE az64
	,primary_flag NUMERIC(28,10)   ENCODE az64
	,contribute_to_step_qty NUMERIC(28,10)   ENCODE az64
	,old_component_sequence_id NUMERIC(15,0)   ENCODE az64
	,operation_seq_num NUMERIC(15,0)   ENCODE az64
	,new_plan_id NUMERIC(15,0)   ENCODE az64
	,new_plan_list VARCHAR(256)   ENCODE lzo
	,applied NUMERIC(28,10)   ENCODE az64
	,simulation_set_id NUMERIC(15,0)   ENCODE az64
	,base_model_item_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64	
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MSC_BOM_COMPONENTS (
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
	kca_operation,
	is_deleted_flg,
	kca_seq_id
	,kca_seq_date
)
 SELECT
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
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID
	,kca_seq_date
    FROM
        bec_ods_stg.MSC_BOM_COMPONENTS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'msc_bom_components';

COMMIT;