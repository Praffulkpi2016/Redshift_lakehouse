/*
# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents full load approach FOR odS.
# File Version: KPI v1.0
*/

begin;

DROP TABLE if exists bec_ods.CST_QUANTITY_LAYERS;

CREATE TABLE IF NOT EXISTS bec_ods.CST_QUANTITY_LAYERS
(

     layer_id NUMERIC(15,0)   ENCODE az64
	,last_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_by NUMERIC(15,0)   ENCODE az64
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by NUMERIC(15,0)   ENCODE az64
	,last_update_login NUMERIC(15,0)   ENCODE az64
	,request_id NUMERIC(15,0)   ENCODE az64
	,program_application_id NUMERIC(15,0)   ENCODE az64
	,program_id NUMERIC(15,0)   ENCODE az64
	,program_update_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,organization_id NUMERIC(15,0)   ENCODE az64
	,inventory_item_id NUMERIC(15,0)   ENCODE az64
	,layer_quantity NUMERIC(28,10)   ENCODE az64
	,create_transaction_id NUMERIC(15,0)   ENCODE az64
	,update_transaction_id NUMERIC(15,0)   ENCODE az64
	,pl_material NUMERIC(28,10)   ENCODE az64
	,pl_material_overhead NUMERIC(28,10)   ENCODE az64
	,pl_resource NUMERIC(15,0)   ENCODE az64
	,pl_outside_processing NUMERIC(28,10)   ENCODE az64
	,pl_overhead NUMERIC(15,0)    ENCODE az64
	,tl_material NUMERIC(28,10)   ENCODE az64
	,tl_material_overhead NUMERIC(28,10)   ENCODE az64
	,tl_resource NUMERIC(15,0)   ENCODE az64
	,tl_outside_processing NUMERIC(28,10)   ENCODE az64
	,tl_overhead NUMERIC(15,0)   ENCODE az64
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
	,cost_group_id NUMERIC(15,0)   ENCODE az64
	,KCA_OPERATION VARCHAR(10)   ENCODE lzo
    ,IS_DELETED_FLG VARCHAR(2) ENCODE lzo
	,kca_seq_id NUMERIC(36,0)   ENCODE az64
	,kca_seq_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)
DISTSTYLE
auto;

INSERT INTO bec_ods.CST_QUANTITY_LAYERS (
    layer_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	organization_id,
	inventory_item_id,
	layer_quantity,
	create_transaction_id,
	update_transaction_id,
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
	cost_group_id,
	kca_operation,
	IS_DELETED_FLG,
	kca_seq_id,
	kca_seq_date
)
    SELECT
    layer_id,
	last_update_date,
	last_updated_by,
	creation_date,
	created_by,
	last_update_login,
	request_id,
	program_application_id,
	program_id,
	program_update_date,
	organization_id,
	inventory_item_id,
	layer_quantity,
	create_transaction_id,
	update_transaction_id,
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
	cost_group_id,
		KCA_OPERATION,
		'N' as IS_DELETED_FLG,
		cast(NULLIF(KCA_SEQ_ID,'') as numeric(36,0)) as KCA_SEQ_ID,
	kca_seq_date
    FROM
        bec_ods_stg.CST_QUANTITY_LAYERS;

end;		

UPDATE bec_etl_ctrl.batch_ods_info
SET
    load_type = 'I',
    last_refresh_date = getdate()
WHERE
    ods_table_name = 'cst_quantity_layers';
commit;