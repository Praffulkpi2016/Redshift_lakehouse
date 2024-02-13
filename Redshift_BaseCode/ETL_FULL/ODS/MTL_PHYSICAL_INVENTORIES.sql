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

DROP TABLE if exists bec_ods.MTL_PHYSICAL_INVENTORIES;

CREATE TABLE IF NOT EXISTS bec_ods.MTL_PHYSICAL_INVENTORIES
(
    physical_inventory_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,physical_inventory_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_adjustment_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,total_adjustment_value NUMERIC(28,10)   ENCODE az64
	,description VARCHAR(50)   ENCODE lzo
	,freeze_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,physical_inventory_name VARCHAR(30)   ENCODE lzo
	,approval_required NUMERIC(15,0)  ENCODE az64
	,all_subinventories_flag NUMERIC(15,0)   ENCODE az64
	,next_tag_number VARCHAR(40)   ENCODE lzo
	,tag_number_increments VARCHAR(40)   ENCODE lzo
	,default_gl_adjust_account NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,approval_tolerance_pos NUMERIC(15,0)   ENCODE az64
	,approval_tolerance_neg NUMERIC(15,0)   ENCODE az64
	,cost_variance_pos NUMERIC(15,0)   ENCODE az64
	,cost_variance_neg NUMERIC(15,0)   ENCODE az64
	,number_of_skus NUMERIC(15,0)   ENCODE az64
	,dynamic_tag_entry_flag NUMERIC(15,0)   ENCODE az64
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
	,attribute_category VARCHAR(150)   ENCODE lzo
	,exclude_zero_balance VARCHAR(1)   ENCODE lzo
	,exclude_negative_balance VARCHAR(1)   ENCODE lzo
	,kca_operation VARCHAR(10)   ENCODE lzo
    ,is_deleted_flg VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.MTL_PHYSICAL_INVENTORIES (
    physical_inventory_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	physical_inventory_date,
	last_adjustment_date,
	total_adjustment_value,
	description,
	freeze_date,
	physical_inventory_name,
	approval_required,
	all_subinventories_flag,
	next_tag_number,
	tag_number_increments,
	default_gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	approval_tolerance_pos,
	approval_tolerance_neg,
	cost_variance_pos,
	cost_variance_neg,
	number_of_skus,
	dynamic_tag_entry_flag,
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
	attribute_category,
	exclude_zero_balance,
	exclude_negative_balance,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date)
   select
		physical_inventory_id,
	organization_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	physical_inventory_date,
	last_adjustment_date,
	total_adjustment_value,
	description,
	freeze_date,
	physical_inventory_name,
	approval_required,
	all_subinventories_flag,
	next_tag_number,
	tag_number_increments,
	default_gl_adjust_account,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	approval_tolerance_pos,
	approval_tolerance_neg,
	cost_variance_pos,
	cost_variance_neg,
	number_of_skus,
	dynamic_tag_entry_flag,
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
	attribute_category,
	exclude_zero_balance,
	exclude_negative_balance,
	kca_operation, 
	'N' as IS_DELETED_FLG,
	cast(nullif(KCA_SEQ_ID, '') as numeric(36, 0)) as KCA_SEQ_ID
	,kca_seq_date
	from
		bec_ods_stg.MTL_PHYSICAL_INVENTORIES ;

end; 
UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'mtl_physical_inventories';
	
commit;