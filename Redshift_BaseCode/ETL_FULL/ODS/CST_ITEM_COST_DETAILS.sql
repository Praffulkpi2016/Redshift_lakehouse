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

DROP TABLE if exists bec_ods.CST_ITEM_COST_DETAILS;

CREATE TABLE IF NOT EXISTS bec_ods.CST_ITEM_COST_DETAILS
(
	inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,cost_type_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,operation_sequence_id NUMERIC(15,0)   ENCODE az64
	,operation_seq_num NUMERIC(15,0)   ENCODE az64
	,department_id NUMERIC(15,0)   ENCODE az64
	,level_type NUMERIC(15,0)   ENCODE az64
	,activity_id NUMERIC(15,0)   ENCODE az64
	,resource_seq_num NUMERIC(15,0)   ENCODE az64
	,resource_id NUMERIC(15,0)   ENCODE az64
	,resource_rate NUMERIC(28,10)   ENCODE az64
	,item_units NUMERIC(28,10)   ENCODE az64
	,activity_units NUMERIC(28,10)   ENCODE az64
	,usage_rate_or_amount NUMERIC(28,10)   ENCODE az64
	,basis_type NUMERIC(15,0)   ENCODE az64
	,basis_resource_id NUMERIC(15,0)   ENCODE az64
	,basis_factor NUMERIC(28,10)   ENCODE az64
	,net_yield_or_shrinkage_factor NUMERIC(28,10)   ENCODE az64
	,item_cost NUMERIC(28,10)   ENCODE az64
	,cost_element_id NUMERIC(15,0)   ENCODE az64
	,rollup_source_type NUMERIC(15,0)   ENCODE az64
	,activity_context VARCHAR(30)   ENCODE lzo
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
	,yielded_cost NUMERIC(28,10)   ENCODE az64
	,source_organization_id NUMERIC(15,0)   ENCODE az64
	,vendor_id NUMERIC(15,0)   ENCODE az64
	,allocation_percent NUMERIC(28,10)   ENCODE az64
	,vendor_site_id NUMERIC(15,0)   ENCODE az64
	,ship_method VARCHAR(30)   ENCODE lzo
	,basis_res_basis_type NUMERIC(15,0)   ENCODE az64
	,bom_component_cost_flag SMALLINT   ENCODE az64
	,cmi_cp_plan_line_id NUMERIC(15,0)   ENCODE az64
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CST_ITEM_COST_DETAILS (
	inventory_item_id,
	organization_id,
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	operation_sequence_id,
	operation_seq_num,
	department_id,
	level_type,
	activity_id,
	resource_seq_num,
	resource_id,
	resource_rate,
	item_units,
	activity_units,
	usage_rate_or_amount,
	basis_type,
	basis_resource_id,
	basis_factor,
	net_yield_or_shrinkage_factor,
	item_cost,
	cost_element_id,
	rollup_source_type,
	activity_context,
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
	yielded_cost,
	source_organization_id,
	vendor_id,
	allocation_percent,
	vendor_site_id,
	ship_method,
	basis_res_basis_type,
	bom_component_cost_flag,
	cmi_cp_plan_line_id,
	kca_operation,
	is_deleted_flg,
	kca_seq_id,
	kca_seq_date
)
    SELECT distinct
	inventory_item_id,
	organization_id,
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	operation_sequence_id,
	operation_seq_num,
	department_id,
	level_type,
	activity_id,
	resource_seq_num,
	resource_id,
	resource_rate,
	item_units,
	activity_units,
	usage_rate_or_amount,
	basis_type,
	basis_resource_id,
	basis_factor,
	net_yield_or_shrinkage_factor,
	item_cost,
	cost_element_id,
	rollup_source_type,
	activity_context,
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
	yielded_cost,
	source_organization_id,
	vendor_id,
	allocation_percent,
	vendor_site_id,
	ship_method,
	basis_res_basis_type,
	bom_component_cost_flag,
	cmi_cp_plan_line_id,
	kca_operation,
	'N' as IS_DELETED_FLG,
	cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CST_ITEM_COST_DETAILS;
end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_item_cost_details';
	
commit;