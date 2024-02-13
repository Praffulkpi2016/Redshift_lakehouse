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
DROP TABLE if exists bec_ods.CST_ITEM_COSTS;
CREATE TABLE IF NOT EXISTS bec_ods.CST_ITEM_COSTS
(
	inventory_item_id NUMERIC(15,0)   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,cost_type_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,inventory_asset_flag NUMERIC(15,0)   ENCODE az64
	,lot_size NUMERIC(15,0)   ENCODE az64
	,based_on_rollup_flag NUMERIC(15,0)   ENCODE az64
	,shrinkage_rate NUMERIC(28,10)   ENCODE az64
	,defaulted_flag NUMERIC(15,0)   ENCODE az64
	,cost_update_id NUMERIC(15,0)   ENCODE az64
	,pl_material NUMERIC(28,10)   ENCODE az64
	,pl_material_overhead NUMERIC(28,10)   ENCODE az64
	,pl_resource NUMERIC(28,10)   ENCODE az64
	,pl_outside_processing NUMERIC(28,10)   ENCODE az64
	,pl_overhead NUMERIC(28,10)   ENCODE az64
	,tl_material NUMERIC(28,10)   ENCODE az64
	,tl_material_overhead NUMERIC(28,10)   ENCODE az64
	,tl_resource NUMERIC(28,10)   ENCODE az64
	,tl_outside_processing NUMERIC(28,10)   ENCODE az64
	,tl_overhead NUMERIC(28,10)   ENCODE az64
	,material_cost NUMERIC(28,10)   ENCODE az64
	,material_overhead_cost NUMERIC(28,10)   ENCODE az64
	,resource_cost NUMERIC(28,10)   ENCODE az64
	,outside_processing_cost NUMERIC(28,10)   ENCODE az64
	,overhead_cost NUMERIC(28,10)   ENCODE az64
	,pl_item_cost NUMERIC(28,10)   ENCODE az64
	,tl_item_cost NUMERIC(28,10)   ENCODE az64
	,item_cost NUMERIC(28,10)   ENCODE az64
	,unburdened_cost NUMERIC(28,10)   ENCODE az64
	,burden_cost NUMERIC(28,10)   ENCODE az64
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
	,rollup_id NUMERIC(15,0)   ENCODE az64
	,assignment_set_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CST_ITEM_COSTS (
    inventory_item_id,
	organization_id,
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_asset_flag,
	lot_size,
	based_on_rollup_flag,
	shrinkage_rate,
	defaulted_flag,
	cost_update_id,
	pl_material,
	pl_material_overhead,
	pl_resource,
	pl_outside_processing,
	pl_overhead,
	tl_material,
	tl_material_overhead,
	tl_resource,
	tl_outside_processing,
	tl_overhead,
	material_cost,
	material_overhead_cost,
	resource_cost,
	outside_processing_cost,
	overhead_cost,
	pl_item_cost,
	tl_item_cost,
	item_cost,
	unburdened_cost,
	burden_cost,
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
	rollup_id,
	assignment_set_id,
	KCA_OPERATION,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
        inventory_item_id,
	organization_id,
	cost_type_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	inventory_asset_flag,
	lot_size,
	based_on_rollup_flag,
	shrinkage_rate,
	defaulted_flag,
	cost_update_id,
	pl_material,
	pl_material_overhead,
	pl_resource,
	pl_outside_processing,
	pl_overhead,
	tl_material,
	tl_material_overhead,
	tl_resource,
	tl_outside_processing,
	tl_overhead,
	material_cost,
	material_overhead_cost,
	resource_cost,
	outside_processing_cost,
	overhead_cost,
	pl_item_cost,
	tl_item_cost,
	item_cost,
	unburdened_cost,
	burden_cost,
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
	rollup_id,
	assignment_set_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CST_ITEM_COSTS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_item_costs';
	
commit;